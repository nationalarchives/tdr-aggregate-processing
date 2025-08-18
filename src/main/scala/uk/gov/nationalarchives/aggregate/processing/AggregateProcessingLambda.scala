package uk.gov.nationalarchives.aggregate.processing

import com.amazonaws.services.lambda.runtime.events.SQSEvent
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage
import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import io.circe._
import io.circe.generic.semiauto.deriveDecoder
import io.circe.parser._
import com.typesafe.scalalogging.Logger
import uk.gov.nationalarchives.aggregate.processing.AggregateProcessingLambda.{AggregateEvent, assetProcessing, logger, s3Utils}
import uk.gov.nationalarchives.aws.utils.s3.{S3Clients, S3Utils}
import com.typesafe.config.{Config, ConfigFactory}
import software.amazon.awssdk.services.s3.model.S3Object
import uk.gov.nationalarchives.aggregate.processing.modules.AssetProcessing

import scala.jdk.CollectionConverters.CollectionHasAsScala

class AggregateProcessingLambda extends RequestHandler[SQSEvent, Unit] {
  implicit val sharepointInputDecoder: Decoder[AggregateEvent] = deriveDecoder[AggregateEvent]

  override def handleRequest(event: SQSEvent, context: Context): Unit = {
    val sqsMessages: Seq[SQSMessage] = event.getRecords.asScala.toList
    sqsMessages.foreach(m => process(m))
  }

  def process(sqsMessage: SQSMessage): List[S3Object] = {
    val event = parseSqsMessage(sqsMessage)
    logger.info(s"Processing assets with prefix ${event.metadataSourceObjectPrefix}")
    val s3Objects = s3Utils.listAllObjectsWithPrefix(event.metadataSourceBucket, event.metadataSourceObjectPrefix)
    logger.info(s"Retrieved ${s3Objects.size} objects with prefix: ${event.metadataSourceObjectPrefix}")
    for (elem <- s3Objects) { assetProcessing.processAsset(event.metadataSourceBucket, elem.key()) }
    /* TODO:
    * - check result from processing assets
    * - send Consignment API request 'addFilesAndMetadata'
    * - create draft metadata CSV if appropriate
    * - call TransferOrchestration with result from processing assets
    * */
    s3Objects
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
  val assetProcessing: AssetProcessing = AssetProcessing.apply()

  case class AggregateEvent(metadataSourceBucket: String, metadataSourceObjectPrefix: String)
}
