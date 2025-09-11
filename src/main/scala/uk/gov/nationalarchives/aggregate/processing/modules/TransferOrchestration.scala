package uk.gov.nationalarchives.aggregate.processing.modules

import cats.effect.IO
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.Logger
import graphql.codegen.types.ConsignmentStatusInput
import io.circe.generic.auto.exportEncoder
import uk.gov.nationalarchives.aggregate.processing.config.ApplicationConfig.getClientSecret
import uk.gov.nationalarchives.aggregate.processing.modules.Common.ConsignmentStatusType
import uk.gov.nationalarchives.aggregate.processing.modules.Common.ProcessType.AssetProcessing
import uk.gov.nationalarchives.aggregate.processing.modules.Common.StateStatusValue.{Completed, CompletedWithIssues, ConsignmentStatusValue, Failed}
import uk.gov.nationalarchives.aggregate.processing.modules.ErrorHandling.BaseError
import uk.gov.nationalarchives.aggregate.processing.modules.TransferOrchestration.{AssetProcessingEvent, BackendChecksStepFunctionInput, OrchestrationResult, TransferError}
import uk.gov.nationalarchives.aggregate.processing.persistence.GraphQlApi
import uk.gov.nationalarchives.aggregate.processing.persistence.GraphQlApi.{backend, keycloakDeployment}
import uk.gov.nationalarchives.aws.utils.stepfunction.StepFunctionClients.sfnAsyncClient
import uk.gov.nationalarchives.aws.utils.stepfunction.StepFunctionUtils

import java.util.UUID
import scala.util.{Failure, Success, Try}

class TransferOrchestration(graphQlApi: GraphQlApi, stepFunctionUtils: StepFunctionUtils, config: Config)(implicit logger: Logger) {

  def orchestrate[T <: Product](orchestrationEvent: T): Try[IO[OrchestrationResult]] = {
    orchestrationEvent match {
      case assetProcessingEvent: AssetProcessingEvent => Success(orchestrateProcessingEvent(assetProcessingEvent))
      case _                                          => Failure(throw new RuntimeException(s"Unrecognized orchestration event: ${orchestrationEvent.getClass.getName}"))
    }
  }

  private def orchestrateProcessingEvent(event: AssetProcessingEvent): IO[OrchestrationResult] = {
    val errors = event.processingErrors
    val consignmentId = event.consignmentId
    val userId = event.userId
    val consignmentStatusValue: ConsignmentStatusValue = if (errors) { Failed }
    else Completed

    if (errors) {
      val transferError = TransferError(consignmentId, s"$AssetProcessing.$CompletedWithIssues", "One or more assets failed to process.")
      ErrorHandling.handleError(transferError, logger)
    } else {
      logger.info(s"Triggering file checks for consignment: $consignmentId")
      val input = BackendChecksStepFunctionInput(consignmentId = consignmentId.toString, s3SourceBucketPrefix = s"$userId/sharepoint/$consignmentId/metadata")
      stepFunctionUtils.startExecution(stateMachineArn = config.getString("sfn.backendChecksArn"), input, name = Some(s"transfer_service_$consignmentId"))
      if (event.suppliedMetadata) {
        // TODO: trigger draft metadata step function
        logger.info(s"Triggering draft metadata validation for consignment: $consignmentId")
      }
    }

    val clientSecret = getClientSecret()
    val statusInput = ConsignmentStatusInput(consignmentId, ConsignmentStatusType.Upload.toString, Some(consignmentStatusValue.toString), Some(event.userId))
    graphQlApi.updateConsignmentStatus(clientSecret, statusInput)
    IO(OrchestrationResult(consignmentId))
  }
}

object TransferOrchestration {
  val config: Config = ConfigFactory.load()
  val logger = Logger[TransferOrchestration]

  case class AssetProcessingEvent(userId: UUID, consignmentId: UUID, processingErrors: Boolean, suppliedMetadata: Boolean)
  case class TransferError(consignmentId: UUID, errorCode: String, errorMessage: String) extends BaseError {
    override def toString: String = {
      s"${this.simpleName}: consignmentId: $consignmentId, errorCode: $errorCode, errorMessage: $errorMessage"
    }
  }

  case class OrchestrationResult(consignmentId: UUID)
  case class BackendChecksStepFunctionInput(consignmentId: String, s3SourceBucketPrefix: String)

  val stepFunctionUtils = StepFunctionUtils(sfnAsyncClient(config.getString("sfn.endpoint")))

  def apply() = new TransferOrchestration(GraphQlApi(), stepFunctionUtils, config)(logger)
}
