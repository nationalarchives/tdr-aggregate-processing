package uk.gov.nationalarchives.aggregate.processing

import com.amazonaws.services.lambda.runtime.events.SQSEvent
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage
import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.Logger
import io.circe._
import io.circe.generic.semiauto.deriveDecoder
import io.circe.parser._
import uk.gov.nationalarchives.aggregate.processing.AggregateProcessingLambda.{AggregateEvent, logger, s3Utils}
import uk.gov.nationalarchives.aggregate.processing.modules.AssetProcessing
import uk.gov.nationalarchives.aws.utils.s3.{S3Clients, S3Utils}

import scala.jdk.CollectionConverters.CollectionHasAsScala

class AggregateProcessingLambda extends RequestHandler[SQSEvent, Unit] {
  implicit val sharepointInputDecoder: Decoder[AggregateEvent] = deriveDecoder[AggregateEvent]

  override def handleRequest(event: SQSEvent, context: Context): Unit = {
    val sqsMessages: Seq[SQSMessage] = event.getRecords.asScala.toList
    sqsMessages.foreach(m => process(m))
  }

  def process(sqsMessage: SQSMessage): Unit = {
    val event = parseSqsMessage(sqsMessage)
    val sourceBucket = event.metadataSourceBucket
    val objectsPrefix = event.metadataSourceObjectPrefix

    val assetProcessor = AssetProcessing()
    logger.info(s"Processing assets with prefix $objectsPrefix")
    val s3Objects = s3Utils.listAllObjectsWithPrefix(sourceBucket, objectsPrefix)
    logger.info(s"Retrieved ${s3Objects.size} objects with prefix: $objectsPrefix")
    val result = s3Objects.map(o => assetProcessor.processAsset(sourceBucket, o.key()))
    if (result.exists(_.processingErrors)) {
      /* TODO:
          - send failed message;
          - update consignment status */
    } else {
      /* TODO:
       - send graphql mutation;
       - trigger backend checks step function;
       - put draft metadata csv and trigger draft metadata step function if contains supplied metadata
       - update consignment status  */
    }
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

  case class AggregateEvent(metadataSourceBucket: String, metadataSourceObjectPrefix: String)
}
