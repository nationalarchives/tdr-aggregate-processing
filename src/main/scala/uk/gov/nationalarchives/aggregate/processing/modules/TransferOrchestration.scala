package uk.gov.nationalarchives.aggregate.processing.modules

import cats.effect.IO
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.Logger
import graphql.codegen.types.ConsignmentStatusInput
import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder
import uk.gov.nationalarchives.aggregate.processing.modules.Common.AssetSource.AssetSource
import uk.gov.nationalarchives.aggregate.processing.modules.Common.ObjectCategory.Records
import uk.gov.nationalarchives.aggregate.processing.modules.Common.ProcessErrorType.EventError
import uk.gov.nationalarchives.aggregate.processing.modules.Common.ProcessErrorValue.Invalid
import uk.gov.nationalarchives.aggregate.processing.modules.Common.ProcessType.{AggregateProcessing, Orchestration}
import uk.gov.nationalarchives.aggregate.processing.modules.Common.StateStatusValue.{Completed, CompletedWithIssues, ConsignmentStatusValue, Failed}
import uk.gov.nationalarchives.aggregate.processing.modules.Common.{ConsignmentStatusType, StateStatusValue}
import uk.gov.nationalarchives.aggregate.processing.modules.ErrorHandling.{BaseError, handleError}
import uk.gov.nationalarchives.aggregate.processing.modules.TransferOrchestration._
import uk.gov.nationalarchives.aggregate.processing.persistence.GraphQlApi
import uk.gov.nationalarchives.aggregate.processing.persistence.GraphQlApi.{backend, keycloakDeployment}
import uk.gov.nationalarchives.aggregate.processing.utilities.NotificationsClient.UploadEvent
import uk.gov.nationalarchives.aggregate.processing.utilities.{KeycloakClient, NotificationsClient}
import uk.gov.nationalarchives.aws.utils.stepfunction.StepFunctionClients.sfnAsyncClient
import uk.gov.nationalarchives.aws.utils.stepfunction.StepFunctionUtils

import java.util.UUID
import scala.concurrent.ExecutionContext.Implicits.global

class TransferOrchestration(
    graphQlApi: GraphQlApi,
    stepFunctionUtils: StepFunctionUtils,
    notificationUtils: NotificationsClient,
    keycloakConfigurations: KeycloakClient,
    config: Config
)(implicit logger: Logger) {

  implicit val backendChecksStepFunctionInputEncoder: Encoder[BackendChecksStepFunctionInput] = deriveEncoder[BackendChecksStepFunctionInput]
  implicit val metadataChecksStepFunctionInputEncoder: Encoder[MetadataChecksStepFunctionInput] = deriveEncoder[MetadataChecksStepFunctionInput]

  def orchestrate[T <: Product](orchestrationEvent: T): IO[OrchestrationResult] = {
    orchestrationEvent match {
      case aggregateProcessingEvent: AggregateProcessingEvent => orchestrateProcessingEvent(aggregateProcessingEvent)
      case _ =>
        val error = TransferError(None, s"$Orchestration.$EventError.$Invalid", s"Unrecognized orchestration event: ${orchestrationEvent.getClass.getName}")
        handleError(error, logger)
        IO(OrchestrationResult(None, success = false, Some(error)))
    }
  }

  private def orchestrateProcessingEvent(event: AggregateProcessingEvent): IO[OrchestrationResult] = {
    val errors = event.processingErrors
    val consignmentId = event.consignmentId
    val userId = event.userId
    val consignmentStatusValue: ConsignmentStatusValue = if (errors) Failed else Completed

    val statusInput = ConsignmentStatusInput(consignmentId, ConsignmentStatusType.Upload.toString, Some(consignmentStatusValue.toString), Some(event.userId))
    for {
      _ <-
        if (errors) {
          val transferError = TransferError(Some(consignmentId), s"$AggregateProcessing.$CompletedWithIssues", "One or more assets failed to process.")
          IO(ErrorHandling.handleError(transferError, logger))
        } else {
          triggerBackendChecksSfn(event) *> triggerDraftMetadataSfn(event)
        }
      updateResult <- graphQlApi.updateConsignmentStatus(statusInput)
      success = updateResult.nonEmpty
      result = OrchestrationResult(Some(consignmentId), success = success)
      getConsignmentDetails <- graphQlApi.getConsignmentDetails(consignmentId)
      userDetails <- IO.fromFuture(IO(keycloakConfigurations.userDetails(userId.toString)))
      _ <- notificationUtils.publishUploadEvent(
        UploadEvent(
          transferringBodyName = getConsignmentDetails.flatMap(_.transferringBodyName).get,
          consignmentReference = getConsignmentDetails.map(_.consignmentReference).get,
          consignmentId = consignmentId.toString,
          status = consignmentStatusValue.toString,
          userId = event.userId.toString,
          userEmail = userDetails.email,
          assetSource = event.assetSource.toString,
          environment = config.getString("environment")
        )
      )
    } yield result
  }

  private def triggerStepFunction[T <: Product: Encoder](arnKey: String, input: T, consignmentId: UUID): IO[Unit] = {
    val stepName = s"transfer_service_$consignmentId"
    val arn = config.getString(arnKey)
    stepFunctionUtils
      .startExecution(arn, input, Some(stepName))
      .handleErrorWith { ex =>
        IO(logger.error(ex.getMessage)) *> IO(
          ErrorHandling.handleError(
            TransferError(Some(consignmentId), s"$AggregateProcessing.$CompletedWithIssues", s"Step function error: ${ex.getMessage}"),
            logger
          )
        )
      }
      .void
  }

  private def triggerBackendChecksSfn(event: AggregateProcessingEvent): IO[Unit] = {
    val consignmentId = event.consignmentId
    val assetSource = event.assetSource.toString
    logger.info(s"Triggering file checks for consignment: $consignmentId")
    triggerStepFunction(
      arnKey = "sfn.backendChecksArn",
      input = BackendChecksStepFunctionInput(consignmentId.toString, s"${event.userId}/$assetSource/$consignmentId/$Records"),
      consignmentId = consignmentId
    )
  }

  private def triggerDraftMetadataSfn(event: AggregateProcessingEvent): IO[Unit] = {
    val consignmentId = event.consignmentId
    if (event.suppliedMetadata) {
      logger.info(s"Triggering draft metadata validation for consignment: $consignmentId")
      val statusInput = ConsignmentStatusInput(consignmentId, ConsignmentStatusType.DraftMetadata.toString, Some(StateStatusValue.InProgress.toString), Some(event.userId))
      for {
        _ <- graphQlApi.addConsignmentStatus(statusInput)
        _ <- triggerStepFunction(
          arnKey = "sfn.metadataChecksArn",
          input = MetadataChecksStepFunctionInput(consignmentId.toString),
          consignmentId = consignmentId
        )
      } yield ()
    } else IO.unit
  }
}

object TransferOrchestration {
  val config: Config = ConfigFactory.load()
  val logger = Logger[TransferOrchestration]

  trait StepFunctionInput {
    def consignmentId: String
  }
  case class BackendChecksStepFunctionInput(consignmentId: String, s3SourceBucketPrefix: String) extends StepFunctionInput
  case class MetadataChecksStepFunctionInput(consignmentId: String, fileName: String = "draft-metadata.csv") extends StepFunctionInput

  case class AggregateProcessingEvent(assetSource: AssetSource, userId: UUID, consignmentId: UUID, processingErrors: Boolean, suppliedMetadata: Boolean)

  case class TransferError(consignmentId: Option[UUID], errorCode: String, errorMessage: String) extends BaseError {
    override def toString: String = {
      s"${this.simpleName}: consignmentId: $consignmentId, errorCode: $errorCode, errorMessage: $errorMessage"
    }
  }

  case class OrchestrationResult(consignmentId: Option[UUID], success: Boolean, error: Option[TransferError] = None)

  val stepFunctionUtils = StepFunctionUtils(sfnAsyncClient(config.getString("sfn.endpoint")))
  val notificationsClient = NotificationsClient(config)
  val keycloakClient = KeycloakClient(config)

  def apply() = new TransferOrchestration(GraphQlApi(), stepFunctionUtils, notificationsClient, keycloakClient, config)(logger)
}
