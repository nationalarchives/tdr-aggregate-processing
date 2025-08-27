package uk.gov.nationalarchives.aggregate.processing.modules

import cats.effect.IO
import com.typesafe.scalalogging.Logger
import graphql.codegen.types.ConsignmentStatusInput
import uk.gov.nationalarchives.aggregate.processing.config.ApplicationConfig.getClientSecret
import uk.gov.nationalarchives.aggregate.processing.graphql.GraphQlApi
import uk.gov.nationalarchives.aggregate.processing.graphql.GraphQlApi.{backend, keycloakDeployment}
import uk.gov.nationalarchives.aggregate.processing.modules.ErrorHandling.BaseError
import uk.gov.nationalarchives.aggregate.processing.modules.TransferOrchestration.{AssetProcessingEvent, OrchestrationResult, TransferError}

import java.util.UUID
import scala.util.{Failure, Success, Try}

class TransferOrchestration(graphQlApi: GraphQlApi)(implicit logger: Logger) {

  def orchestrate[T <: Product](orchestrationEvent: T): Try[IO[OrchestrationResult]] = {
    orchestrationEvent match {
      case assetProcessingEvent: AssetProcessingEvent => Success(orchestrateProcessingEvent(assetProcessingEvent))
      case _                                          => Failure(throw new RuntimeException(s"Unrecognized orchestration event: ${orchestrationEvent.getClass.getName}"))
    }
  }

  private def orchestrateProcessingEvent(event: AssetProcessingEvent): IO[OrchestrationResult] = {
    val errors = event.processingErrors
    val consignmentId = event.consignmentId
    val consignmentStatusValue = if (errors) {
      Some("Failed")
    } else Some("Completed")

    if (errors) {
      val transferError = TransferError(consignmentId, "ASSET_PROCESSING.completedWithIssues", "One or more assets failed to process.")
      ErrorHandling.handleError(transferError, logger)
    } else {
      logger.info(s"Triggering file checks for consignment: $consignmentId")
      // TODO: trigger backend checks step functions
      if (event.suppliedMetadata) {
        logger.info(s"Triggering draft metadata validation for consignment: $consignmentId")
      }
    }

    val clientSecret = getClientSecret()
    val statusInput = ConsignmentStatusInput(consignmentId, "Upload", consignmentStatusValue, Some(event.userId))
    graphQlApi.updateConsignmentStatus(clientSecret, statusInput)
    IO(OrchestrationResult(consignmentId))
  }
}

object TransferOrchestration {
  val logger = Logger[TransferOrchestration]

  case class AssetProcessingEvent(userId: UUID, consignmentId: UUID, processingErrors: Boolean, suppliedMetadata: Boolean)
  case class TransferError(consignmentId: UUID, errorCode: String, errorMessage: String) extends BaseError {
    override def toString: String = {
      s"${TransferError.getClass.getSimpleName}: consignmentId: $consignmentId, errorCode: $errorCode, errorMessage: $errorMessage"
    }
  }

  case class OrchestrationResult(consignmentId: UUID)

  def apply() = new TransferOrchestration(GraphQlApi())(logger)
}
