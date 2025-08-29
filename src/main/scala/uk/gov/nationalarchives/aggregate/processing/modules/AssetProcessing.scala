package uk.gov.nationalarchives.aggregate.processing.modules

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.Logger
import graphql.codegen.types.ClientSideMetadataInput
import io.circe.{Json, parser}
import uk.gov.nationalarchives.aggregate.processing.modules.AssetProcessing._
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

  def processAsset(s3Bucket: String, objectKey: String): AssetProcessingResult = {
    logger.info(s"Processing asset metadata for: $objectKey")
    val assetDetails = parseObjectKey(objectKey)
    if (assetDetails.isEmpty) {
      val invalidObjectKeyError = AssetProcessingError(None, None, None, "ASSET_PROCESSING.OBJECT_KEY.INVALID", s"Invalid object key: $objectKey")
      ErrorHandling.handleError(invalidObjectKeyError, logger)
      AssetProcessingResult(None, processingErrors = true, None)
    } else {
      val matchId = assetDetails.get.matchId
      val metadataObjectParseResult = parseMetadataObject(s3Utils, s3Bucket, assetDetails.get)
      if (metadataObjectParseResult.json.isEmpty) {
        AssetProcessingResult(Some(matchId), metadataObjectParseResult.errors, None)
      } else {
        val metadataJson = metadataObjectParseResult.json.get
        val input = toClientSideMetadataInput(metadataJson, assetDetails.get)
        val suppliedMetadata = toSuppliedMetadata(metadataJson)
        val processingErrors = metadataObjectParseResult.errors || input.isEmpty
        AssetProcessingResult(Some(matchId), processingErrors, input, suppliedMetadata)
      }
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

  private def toClientSideMetadataInput(metadataJson: Json, assetDetails: AssetDetails): Option[ClientSideMetadataInput] = {
    val requiredFieldNames = Set("matchId", "FileRef", "sha256ClientSideChecksum", "Modified", "Length") // TODO: retrieve from schema
    val retrievedJsonFields = getJsonField(requiredFieldNames, metadataJson)
    if (retrievedJsonFields.size != requiredFieldNames.size) {
      requiredFieldNames.toList
        .diff(retrievedJsonFields.toList)
        .foreach(f => {
          val error =
            AssetProcessingError(Some(assetDetails.consignmentId.toString), Some(assetDetails.matchId), Some(assetDetails.source), s"ASSET_PROCESSING.MISSING_FIELD.$f", "")
          ErrorHandling.handleError(error, logger)
        })
      None
    } else {
      // TODO: provide mapping in config json for input field names
      val lastModified = t"${retrievedJsonFields("Modified")}".getTime
      val fileSize = retrievedJsonFields("Length").toLong
      Some(ClientSideMetadataInput(retrievedJsonFields("FileRef"), retrievedJsonFields("sha256ClientSideChecksum"), lastModified, fileSize, retrievedJsonFields("matchId")))
    }
  }

  private def toSuppliedMetadata(metadataJson: Json): List[SuppliedMetadata] = {
    // TODO: check for any supplied metadata fields
    Nil
  }

  private def parseMetadataObject(s3Utils: S3Utils, s3Bucket: String, assetDetails: AssetDetails): MetadataObjectParseResult = {
    Try(s3Utils.getObjectAsStream(s3Bucket, assetDetails.objectKey)) match {
      case Failure(ex) =>
        val s3ReadError = generateErrorMessage(assetDetails, "ASSET_PROCESSING.S3.READ_ERROR", ex)
        ErrorHandling.handleError(s3ReadError, logger)
        MetadataObjectParseResult(errors = true, None)
      case Success(inputStream) =>
        val utfError = validUTF8(inputStream, assetDetails.objectKey)
        inputStream.reset()
        val json = parser.parse(inputStream.readAllBytes().map(_.toChar).mkString) match {
          case Left(failure) =>
            val jsonError = generateErrorMessage(assetDetails, "ASSET_PROCESSING.JSON.INVALID", failure)
            ErrorHandling.handleError(jsonError, logger)
            None
          case Right(json) => Some(json)
        }
        val hasErrors = utfError || json.isEmpty
        MetadataObjectParseResult(hasErrors, json)
    }
  }

  private def validUTF8(inputStream: java.io.InputStream, objectKey: String): Boolean = {
    val utf8Validator = new Utf8Validator(new UTF8ValidationHandler()(logger))
    Try(utf8Validator.validate(inputStream)) match {
      case Failure(ex) =>
        val utf8Error = generateErrorMessage(parseObjectKey(objectKey).get, "ASSET_PROCESSING.UTF.INVALID", ex)
        ErrorHandling.handleError(utf8Error, logger)
        true
      case _ =>
        logger.info(s"UTF8 validation passed: $objectKey")
        false
    }
  }

  private def generateErrorMessage(assetDetails: AssetDetails, errorCode: String, errorMsg: Throwable): AssetProcessingError = {
    AssetProcessingError(
      consignmentId = Some(assetDetails.consignmentId.toString),
      matchId = Some(assetDetails.matchId),
      source = Some(assetDetails.source),
      errorCode = errorCode,
      errorMsg = errorMsg.getMessage
    )
  }
}

object AssetProcessing {
  private case class AssetDetails(consignmentId: UUID, matchId: String, source: String, objectKey: String)
  private case class MetadataObjectParseResult(errors: Boolean, json: Option[Json])
  case class SuppliedMetadata(propertyName: String, propertyValue: String)
  case class AssetProcessingResult(
      matchId: Option[String],
      processingErrors: Boolean = false,
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

  private def parseObjectKey(objectKey: String): Option[AssetDetails] = {
    Try {
      val elements = objectKey.split('/')
      val consignmentId = UUID.fromString(elements(2))
      val matchId = elements.last.split("\\.")(0)
      val source = elements(1)
      AssetDetails(consignmentId, matchId, source, objectKey)
    } match {
      case Failure(ex) => None
      case Success(ad) => Some(ad)
    }
  }

  def apply() = new AssetProcessing(s3Utils)(logger)
}
