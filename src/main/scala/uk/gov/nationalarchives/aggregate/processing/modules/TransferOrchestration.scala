package uk.gov.nationalarchives.aggregate.processing.modules

import cats.effect.IO
import com.typesafe.scalalogging.Logger
import graphql.codegen.types.ConsignmentStatusInput
import uk.gov.nationalarchives.aggregate.processing.modules.Common.ConsignmentStatusType
import uk.gov.nationalarchives.aggregate.processing.modules.Common.ProcessErrorType.EventError
import uk.gov.nationalarchives.aggregate.processing.modules.Common.ProcessErrorValue.Invalid
import uk.gov.nationalarchives.aggregate.processing.modules.Common.ProcessType.{AggregateProcessing, Orchestration}
import uk.gov.nationalarchives.aggregate.processing.modules.Common.StateStatusValue.{Completed, CompletedWithIssues, ConsignmentStatusValue, Failed}
import uk.gov.nationalarchives.aggregate.processing.modules.ErrorHandling.{BaseError, handleError}
import uk.gov.nationalarchives.aggregate.processing.modules.TransferOrchestration.{AggregateProcessingEvent, OrchestrationResult, TransferError}
import uk.gov.nationalarchives.aggregate.processing.persistence.GraphQlApi
import uk.gov.nationalarchives.aggregate.processing.persistence.GraphQlApi.{backend, keycloakDeployment}

import java.util.UUID

class TransferOrchestration(graphQlApi: GraphQlApi)(implicit logger: Logger) {

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
    val consignmentStatusValue: ConsignmentStatusValue = if (errors) { Failed }
    else Completed

    if (errors) {
      val transferError = TransferError(Some(consignmentId), s"$AggregateProcessing.$CompletedWithIssues", "One or more assets failed to process.")
      ErrorHandling.handleError(transferError, logger)
    } else {
      logger.info(s"Triggering file checks for consignment: $consignmentId")
      // TODO: trigger backend checks step function
      if (event.suppliedMetadata) {
        // TODO: trigger draft metadata step function
        logger.info(s"Triggering draft metadata validation for consignment: $consignmentId")
      }
    }

    val statusInput = ConsignmentStatusInput(consignmentId, ConsignmentStatusType.Upload.toString, Some(consignmentStatusValue.toString), Some(event.userId))
    for {
      updateResult <- graphQlApi.updateConsignmentStatus(statusInput)
      success = updateResult.nonEmpty
    } yield OrchestrationResult(Some(consignmentId), success = success)
  }
}

object TransferOrchestration {
  val logger = Logger[TransferOrchestration]

  case class AggregateProcessingEvent(userId: UUID, consignmentId: UUID, processingErrors: Boolean, suppliedMetadata: Boolean)
  case class TransferError(consignmentId: Option[UUID], errorCode: String, errorMessage: String) extends BaseError {
    override def toString: String = {
      s"${this.simpleName}: consignmentId: $consignmentId, errorCode: $errorCode, errorMessage: $errorMessage"
    }
  }

  case class OrchestrationResult(consignmentId: Option[UUID], success: Boolean, error: Option[TransferError] = None)

  def apply() = new TransferOrchestration(GraphQlApi())(logger)
}
