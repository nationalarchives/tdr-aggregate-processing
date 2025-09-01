package uk.gov.nationalarchives.aggregate.processing.modules

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.Logger
import graphql.codegen.types.ClientSideMetadataInput
import io.circe.{Json, parser}
import uk.gov.nationalarchives.aggregate.processing.modules.AssetProcessing._
import uk.gov.nationalarchives.aggregate.processing.modules.Common.ProcessErrorType.{EncodingError, JsonError, MetadataFieldError, ObjectKeyParsingError, S3Error => s3e}
import uk.gov.nationalarchives.aggregate.processing.modules.Common.ProcessErrorValue.{Invalid, Missing, ReadError}
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
      val elements = objectKey.split('/')
      val userId = UUID.fromString(elements(0))
      val source = elements(1)
      val consignmentId = UUID.fromString(elements(2))

      val objectElements = elements.last.split("\\.")
      val matchId = objectElements(0)
      val objectType = objectElements(1)

      AssetProcessingEvent(userId, consignmentId, matchId, source, objectType, s3Bucket, objectKey)
    }.fold(
      ex => {
        val error = AssetProcessingError(None, None, None, s"$ptAp.$ObjectKeyParsingError.$Invalid", s"Invalid object key: $objectKey: ${ex.getMessage}")
        handleProcessError(error)
      },
      event => parseMetadataObject(s3Utils, s3Bucket, event)
    )
  }

  private def parseMetadataObject(s3Utils: S3Utils, s3Bucket: String, event: AssetProcessingEvent): AssetProcessingResult = {
    Try(s3Utils.getObjectAsStream(s3Bucket, event.objectKey)).fold(
      ex => {
        val error = generateErrorMessage(event, s"$ptAp.$s3e.$ReadError", ex.getMessage)
        handleProcessError(error)
      },
      inputStream => {
        validUTF8(inputStream, event)
      }
    )
  }

  private def validUTF8(inputStream: java.io.InputStream, event: AssetProcessingEvent): AssetProcessingResult = {
    val utf8Validator = new Utf8Validator(new UTF8ValidationHandler())
    Try(utf8Validator.validate(inputStream)).fold(
      ex => {
        val error = generateErrorMessage(event, s"$ptAp.$EncodingError.$Invalid", ex.getMessage)
        handleProcessError(error)
      },
      _ => {
        inputStream.reset()
        parser
          .parse(inputStream.readAllBytes().map(_.toChar).mkString)
          .fold(
            ex => {
              val error = generateErrorMessage(event, s"$ptAp.$JsonError.$Invalid", ex.getMessage())
              handleProcessError(error)
            },
            json => parseMetadataJson(json, event)
          )
      }
    )
  }

  private def parseMetadataJson(metadataJson: Json, event: AssetProcessingEvent): AssetProcessingResult = {
    val requiredFieldNames = Set("matchId", "FileRef", "sha256ClientSideChecksum", "Modified", "Length") // TODO: retrieve from schema
    val retrievedJsonFields = getJsonField(requiredFieldNames, metadataJson)
    val matchId = event.matchId
    val objectKey = event.objectKey

    if (retrievedJsonFields.size != requiredFieldNames.size) {
      val missingFields = requiredFieldNames.toList.diff(retrievedJsonFields.keys.toList)
      val errorMessage: String = s"Missing fields: ${missingFields.mkString(",")}"
      val error = generateErrorMessage(event, s"$ptAp.$MetadataFieldError.$Missing", errorMessage)
      handleProcessError(error)
    } else {
      val suppliedMetadata = toSuppliedMetadata(metadataJson)
      // TODO; handle conversion errors gracefully
      val lastModified = t"${retrievedJsonFields("Modified")}".getTime
      val fileSize = retrievedJsonFields("Length").toLong
      // TODO: provide mapping in config json for input field names
      val clientSideMetadataInput =
        ClientSideMetadataInput(retrievedJsonFields("FileRef"), retrievedJsonFields("sha256ClientSideChecksum"), lastModified, fileSize, retrievedJsonFields("matchId"))

      logger.info(s"Asset metadata successfully processed for: $objectKey")
      AssetProcessingResult(Some(matchId), processingErrors = false, Some(clientSideMetadataInput), suppliedMetadata)
    }
  }

  private def getJsonField(fieldNames: Set[String], metadataJson: Json): Map[String, String] = {
    fieldNames
      .flatMap(field =>
        Try(metadataJson.\\(field).head.asString.get) match {
          case Failure(ex)    => None
          case Success(value) => Some(field -> value)
        }
      )
      .toMap
  }

  private def toSuppliedMetadata(metadataJson: Json): List[SuppliedMetadata] = {
    // TODO: check for any supplied metadata fields
    Nil
  }

  private def generateErrorMessage(event: AssetProcessingEvent, errorCode: String, errorMessage: String): AssetProcessingError = {
    AssetProcessingError(
      consignmentId = Some(event.consignmentId.toString),
      matchId = Some(event.matchId),
      source = Some(event.source),
      errorCode = errorCode,
      errorMsg = errorMessage
    )
  }
}

object AssetProcessing {
  private case class AssetProcessingEvent(userId: UUID, consignmentId: UUID, matchId: String, source: String, objectType: String, s3SourceBucket: String, objectKey: String)
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
