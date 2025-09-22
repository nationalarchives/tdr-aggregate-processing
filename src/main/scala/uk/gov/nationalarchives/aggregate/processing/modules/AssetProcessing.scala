package uk.gov.nationalarchives.aggregate.processing.modules

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.Logger
import graphql.codegen.types.ClientSideMetadataInput
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder, Json, parser}
import uk.gov.nationalarchives.aggregate.processing.modules.AssetProcessing._
import uk.gov.nationalarchives.aggregate.processing.modules.Common.AssetSource.AssetSource
import uk.gov.nationalarchives.aggregate.processing.modules.Common.ObjectType
import uk.gov.nationalarchives.aggregate.processing.modules.Common.ObjectType.ObjectType
import uk.gov.nationalarchives.aggregate.processing.modules.Common.ProcessErrorType.{EncodingError, JsonError, ObjectKeyParsingError, S3Error => s3e}
import uk.gov.nationalarchives.aggregate.processing.modules.Common.ProcessErrorValue.{Invalid, ReadError}
import uk.gov.nationalarchives.aggregate.processing.modules.Common.ProcessType.{AssetProcessing => ptAp}
import uk.gov.nationalarchives.aggregate.processing.modules.ErrorHandling.BaseError
import uk.gov.nationalarchives.aggregate.processing.utilities.UTF8ValidationHandler
import uk.gov.nationalarchives.aws.utils.s3.{S3Clients, S3Utils}
import uk.gov.nationalarchives.utf8.validator.Utf8Validator

import java.sql.Timestamp
import java.time.Instant
import java.util.UUID
import scala.util.{Failure, Success, Try}

class AssetProcessing(s3Utils: S3Utils)(implicit logger: Logger) {
  implicit class StringTimeConversions(sc: StringContext) {
    def t(args: Any*): Timestamp =
      Timestamp.from(Instant.parse(sc.s(args: _*)))
  }

  private def handleProcessError(error: AssetProcessingError): AssetProcessingResult = {
    ErrorHandling.handleError(error, logger)
    AssetProcessingResult(error.matchId, processingErrors = true, None)
  }

  def processAsset(s3Bucket: String, objectKey: String): AssetProcessingResult = {
    logger.info(s"Processing asset metadata for: $objectKey")
    handleEvent(s3Bucket, objectKey)
  }

  private def handleEvent(s3Bucket: String, objectKey: String): AssetProcessingResult = {
    Try {
      val objectDetails = Common.objectKeyParser(objectKey)
      val objectElements = objectDetails.objectElements.get.split("\\.")
      val matchId = objectElements(0)
      val objectType = ObjectType.withName(objectElements(1))
      AssetProcessingEvent(objectDetails.userId, objectDetails.consignmentId, matchId, objectDetails.assetSource, objectType, s3Bucket, objectKey)
    } match {
      case Failure(ex) =>
        val error = AssetProcessingError(None, None, None, s"$ptAp.$ObjectKeyParsingError.$Invalid", s"Invalid object key: $objectKey: ${ex.getMessage}")
        handleProcessError(error)
      case Success(event) => parseMetadataObject(s3Utils, event)
    }
  }

  private def parseMetadataObject(s3Utils: S3Utils, event: AssetProcessingEvent): AssetProcessingResult = {
    // TODO: check for threat found
    Try(s3Utils.getObjectAsStream(event.s3SourceBucket, event.objectKey)) match {
      case Failure(ex) =>
        val error = generateErrorMessage(event, s"$ptAp.$s3e.$ReadError", ex.getMessage)
        handleProcessError(error)
      case Success(inputStream) => validUTF8(inputStream, event)
    }
  }

  private def validUTF8(inputStream: java.io.InputStream, event: AssetProcessingEvent): AssetProcessingResult = {
    val utf8Validator = new Utf8Validator(new UTF8ValidationHandler())
    Try {
      utf8Validator.validate(inputStream)
      inputStream.reset()
      parser.parse(inputStream.readAllBytes().map(_.toChar).mkString)
    } match {
      case Failure(utfEx) =>
        val error = generateErrorMessage(event, s"$ptAp.$EncodingError.$Invalid", utfEx.getMessage)
        handleProcessError(error)
      case Success(Left(parseEx)) =>
        val error = generateErrorMessage(event, s"$ptAp.$JsonError.$Invalid", parseEx.getMessage())
        handleProcessError(error)
      case Success(Right(json)) =>
        parseMetadataJson(json, event)
    }
  }

  private def parseMetadataJson(metadataJson: Json, event: AssetProcessingEvent): AssetProcessingResult = {
    val matchId = event.matchId
    val objectKey = event.objectKey
    val suppliedMetadata = toSuppliedMetadata(metadataJson)
    metadataJson
      .as[RequiredSharePointMetadata]
      .fold(
        ex => {
          val error = AssetProcessingError(Some(event.consignmentId.toString), Some(event.matchId), Some(event.source.toString), s"$ptAp.$JsonError.$Invalid", ex.getMessage())
          handleProcessError(error)
        },
        metadata => {
          if (event.matchId != metadata.matchId) {
            val error = AssetProcessingError(
              Some(event.consignmentId.toString),
              None,
              Some(event.source.toString),
              s"$ptAp.MATCH_ID.MISMATCH",
              s"Mismatched match ids: ${event.matchId} and ${metadata.matchId}"
            )
            handleProcessError(error)
          } else {
            val dateLastModified = t"${metadata.Modified}".getTime

            val input = ClientSideMetadataInput(metadata.FileRef, metadata.SHA256ClientSideChecksum, dateLastModified, metadata.Length, metadata.matchId)
            logger.info(s"Asset metadata successfully processed for: $objectKey")
            AssetProcessingResult(Some(matchId), processingErrors = false, Some(input), suppliedMetadata)
          }
        }
      )
  }

  private def toSuppliedMetadata(metadataJson: Json): List[SuppliedMetadata] = {
    // TODO: check for any supplied metadata fields
    Nil
  }

  private def generateErrorMessage(event: AssetProcessingEvent, errorCode: String, errorMessage: String): AssetProcessingError = {
    AssetProcessingError(
      consignmentId = Some(event.consignmentId.toString),
      matchId = Some(event.matchId),
      source = Some(event.source.toString),
      errorCode = errorCode,
      errorMsg = errorMessage
    )
  }
}

object AssetProcessing {
  implicit val sharePointDecoder: Decoder[RequiredSharePointMetadata] = deriveDecoder[RequiredSharePointMetadata]
  implicit val sharePointEncoder: Encoder[RequiredSharePointMetadata] = deriveEncoder[RequiredSharePointMetadata]

  trait MetadataSideCar {}
  case class RequiredSharePointMetadata(matchId: String, transferId: UUID, Modified: String, SHA256ClientSideChecksum: String, Length: Long, FileRef: String, FileLeafRef: String)
      extends MetadataSideCar

  private case class AssetProcessingEvent(
      userId: UUID,
      consignmentId: UUID,
      matchId: String,
      source: AssetSource,
      objectType: ObjectType,
      s3SourceBucket: String,
      objectKey: String
  )
  case class SuppliedMetadata(propertyName: String, propertyValue: String)
  case class AssetProcessingResult(
      matchId: Option[String],
      processingErrors: Boolean,
      clientSideMetadataInput: Option[ClientSideMetadataInput],
      suppliedMetadata: List[SuppliedMetadata] = List()
  )
  case class AssetProcessingError(consignmentId: Option[String], matchId: Option[String], source: Option[String], errorCode: String, errorMsg: String) extends BaseError {
    override def toString: String = {
      s"${this.simpleName}: consignmentId: $consignmentId, matchId: $matchId, source: $source, errorCode: $errorCode, errorMessage: $errorMsg"
    }
  }

  val logger: Logger = Logger[AssetProcessing]

  private val configFactory: Config = ConfigFactory.load()
  val s3Utils: S3Utils = S3Utils(S3Clients.s3Async(configFactory.getString("s3.endpoint")))
  def apply() = new AssetProcessing(s3Utils)(logger)
}
