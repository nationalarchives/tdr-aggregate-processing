package uk.gov.nationalarchives.aggregate.processing

import cats.effect.IO
import cats.effect.IO._
import cats.effect.unsafe.implicits.global
import com.amazonaws.services.lambda.runtime.events.SQSEvent
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage
import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.Logger
import graphql.codegen.types.AddFileAndMetadataInput
import io.circe._
import io.circe.generic.semiauto.deriveDecoder
import io.circe.parser._
import uk.gov.nationalarchives.aggregate.processing.AggregateProcessingLambda._
import uk.gov.nationalarchives.aggregate.processing.modules.Common.ProcessType.AggregateProcessing
import uk.gov.nationalarchives.aggregate.processing.modules.ErrorHandling.{BaseError, handleError}
import uk.gov.nationalarchives.aggregate.processing.modules.TransferOrchestration.{AggregateProcessingEvent, OrchestrationResult}
import uk.gov.nationalarchives.aggregate.processing.modules.{AssetProcessing, Common, TransferOrchestration}
import uk.gov.nationalarchives.aggregate.processing.persistence.GraphQlApi
import uk.gov.nationalarchives.aggregate.processing.persistence.GraphQlApi.{backend, keycloakDeployment}
import uk.gov.nationalarchives.aws.utils.s3.{S3Clients, S3Utils}

import java.util.UUID
import scala.jdk.CollectionConverters.CollectionHasAsScala

class AggregateProcessingLambda extends RequestHandler[SQSEvent, Unit] {
  implicit val sharepointInputDecoder: Decoder[AggregateEvent] = deriveDecoder[AggregateEvent]
  private val assetProcessor = AssetProcessing()
  private val persistenceApi = GraphQlApi()

  override def handleRequest(event: SQSEvent, context: Context): Unit = {
    val sqsMessages: Seq[SQSMessage] = event.getRecords.asScala.toList
    val events = sqsMessages.map(message => parseSqsMessage(message))

    val resultsIO = events.map(event =>
      processEvent(event).handleErrorWith(err => {
        val details = Common.objectKeyParser(event.metadataSourceObjectPrefix)
        val error = AggregateProcessingError(details.consignmentId, s"$AggregateProcessing", err.getMessage)
        handleError(error, logger)
        IO.unit
      })
    )

    resultsIO.parSequence.unsafeRunSync()
  }

  def processEvent(event: AggregateEvent): IO[TransferOrchestration.OrchestrationResult] = {
    val sourceBucket = event.metadataSourceBucket
    val objectsPrefix = event.metadataSourceObjectPrefix
    val objectKeyPrefixDetails = Common.objectKeyParser(objectsPrefix)
    val consignmentId = objectKeyPrefixDetails.consignmentId
    val userId = objectKeyPrefixDetails.userId
    logger.info(s"Processing assets with prefix $objectsPrefix")
    val s3Objects = s3Utils.listAllObjectsWithPrefix(sourceBucket, objectsPrefix)
    logger.info(s"Retrieved ${s3Objects.size} objects with prefix: $objectsPrefix")
    val assetProcessingResults = s3Objects.map(o => assetProcessor.processAsset(sourceBucket, o.key()))
    val clientSideMetadataInput = assetProcessingResults.flatMap(_.clientSideMetadataInput)
    val assetProcessingErrors = assetProcessingResults.exists(_.processingErrors)

    if (assetProcessingErrors) {
      val orchestrationEvent = AggregateProcessingEvent(userId, consignmentId, processingErrors = true, suppliedMetadata = false)
      for {
        orchestrationResult <- sendOrchestrationEvent(orchestrationEvent)
      } yield orchestrationResult
    } else {
      val input = AddFileAndMetadataInput(consignmentId, clientSideMetadataInput, None, Some(userId))
      for {
        _ <- persistenceApi.addClientSideMetadata(input)
        suppliedMetadata = assetProcessingResults.exists(_.suppliedMetadata.nonEmpty)
        orchestrationEvent = AggregateProcessingEvent(userId, consignmentId, processingErrors = false, suppliedMetadata = suppliedMetadata)
        orchestrationResult <- sendOrchestrationEvent(orchestrationEvent)
      } yield orchestrationResult
    }
  }

  private def sendOrchestrationEvent(event: AggregateProcessingEvent): IO[OrchestrationResult] = {
    // TODO: handle errors from orchestration gracefully
    TransferOrchestration().orchestrate(event).get
  }

  private def parseSqsMessage(sqsMessage: SQSMessage): AggregateEvent = {
    parse(sqsMessage.getBody) match {
      case Left(parsingError) =>
        throw new IllegalArgumentException(s"Invalid JSON object: ${parsingError.message}")
      case Right(json) =>
        decode[AggregateEvent](json.toString()) match {
          case Left(decodingError) => throw new IllegalArgumentException(s"Invalid event: ${decodingError.getMessage}")
          case Right(event)        => event
        }
    }
  }
}

object AggregateProcessingLambda {
  val logger: Logger = Logger[AggregateProcessingLambda]
  private val configFactory: Config = ConfigFactory.load()
  val s3Utils: S3Utils = S3Utils(S3Clients.s3Async(configFactory.getString("s3.endpoint")))

  private case class AggregateProcessingError(consignmentId: UUID, errorCode: String, errorMessage: String) extends BaseError
  case class AggregateEvent(metadataSourceBucket: String, metadataSourceObjectPrefix: String)
}
