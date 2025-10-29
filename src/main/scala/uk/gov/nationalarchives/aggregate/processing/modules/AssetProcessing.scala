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
import uk.gov.nationalarchives.aggregate.processing.modules.Common.ProcessErrorType.{EncodingError, JsonError, MatchIdError, ObjectKeyParsingError, S3Error => s3e}
import uk.gov.nationalarchives.aggregate.processing.modules.Common.ProcessErrorValue.{Invalid, Mismatch, ReadError}
import uk.gov.nationalarchives.aggregate.processing.modules.Common.ProcessType.{AssetProcessing => ptAp}
import uk.gov.nationalarchives.aggregate.processing.modules.Common.StateStatusValue.{Completed, CompletedWithIssues}
import uk.gov.nationalarchives.aggregate.processing.modules.ErrorHandling.BaseError
import uk.gov.nationalarchives.aggregate.processing.utilities.UTF8ValidationHandler
import uk.gov.nationalarchives.aws.utils.s3.{S3Clients, S3Utils}
import uk.gov.nationalarchives.tdr.schemautils.ConfigUtils
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

  private lazy val metadataConfig: ConfigUtils.MetadataConfiguration = ConfigUtils.loadConfiguration
  private lazy val tdrDataLoadHeaderToPropertyMapper: String => String = metadataConfig.propertyToOutputMapper("tdrFileHeader")
  private lazy val keyToSharepointHeader: String => String = metadataConfig.propertyToOutputMapper("sharePointTag")

  private def handleProcessError(error: AssetProcessingError, s3Bucket: String, s3ObjectKey: String): AssetProcessingResult = {
    ErrorHandling.handleError(error, logger)
    val errorTags: Map[String, String] = Map(ptAp.toString -> CompletedWithIssues.toString)
    s3Utils.addObjectTags(s3Bucket, s3ObjectKey, errorTags)
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
        handleProcessError(error, s3Bucket, objectKey)
      case Success(event) => parseMetadataObject(s3Utils, event)
    }
  }

  private def parseMetadataObject(s3Utils: S3Utils, event: AssetProcessingEvent): AssetProcessingResult = {
    // TODO: check for threat found
    val s3Bucket = event.s3SourceBucket
    val objectKey = event.objectKey
    Try(s3Utils.getObjectAsStream(s3Bucket, objectKey)) match {
      case Failure(ex) =>
        val error = generateErrorMessage(event, s"$ptAp.$s3e.$ReadError", ex.getMessage)
        handleProcessError(error, s3Bucket, objectKey)
      case Success(inputStream) => validUTF8(inputStream, event)
    }
  }

  private def validUTF8(inputStream: java.io.InputStream, event: AssetProcessingEvent): AssetProcessingResult = {
    val utf8Validator = new Utf8Validator(new UTF8ValidationHandler())
    val s3Bucket = event.s3SourceBucket
    val objectKey = event.objectKey
    Try {
      utf8Validator.validate(inputStream)
      inputStream.reset()
      parser.parse(inputStream.readAllBytes().map(_.toChar).mkString)
    } match {
      case Failure(utfEx) =>
        val error = generateErrorMessage(event, s"$ptAp.$EncodingError.$Invalid", utfEx.getMessage)
        handleProcessError(error, s3Bucket, objectKey)
      case Success(Left(parseEx)) =>
        val error = generateErrorMessage(event, s"$ptAp.$JsonError.$Invalid", parseEx.getMessage())
        handleProcessError(error, s3Bucket, objectKey)
      case Success(Right(json)) =>
        parseMetadataJson(json, event)
    }
  }

  private def parseMetadataJson(metadataJson: Json, event: AssetProcessingEvent): AssetProcessingResult = {
    val matchId = event.matchId
    val objectKey = event.objectKey
    val s3Bucket = event.s3SourceBucket
    metadataJson
      .as[RequiredSharePointMetadata]
      .fold(
        ex => {
          val error = AssetProcessingError(Some(event.consignmentId.toString), Some(event.matchId), Some(event.source.toString), s"$ptAp.$JsonError.$Invalid", ex.getMessage())
          handleProcessError(error, s3Bucket, objectKey)
        },
        metadata => {
          if (event.matchId != metadata.matchId) {
            val error = AssetProcessingError(
              Some(event.consignmentId.toString),
              None,
              Some(event.source.toString),
              s"$ptAp.$MatchIdError.$Mismatch",
              s"Mismatched match ids: ${event.matchId} and ${metadata.matchId}"
            )
            handleProcessError(error, s3Bucket, objectKey)
          } else {
            val suppliedProperties: Seq[String] = metadataConfig.getPropertiesByPropertyType("Supplied").map(p => tdrDataLoadHeaderToPropertyMapper(p))
            val systemProperties: Seq[String] = metadataConfig.getPropertiesByPropertyType("System").map(p => keyToSharepointHeader(p))

            val systemMetadata = toMetadataProperties(metadataJson, systemProperties)
            val suppliedMetadata = toMetadataProperties(metadataJson, suppliedProperties)

            val dateLastModified = t"${metadata.Modified}".getTime
            val sharePointLocation = sharePointLocationPathToFilePath(metadata.FileRef)
            val normalizedTarget = sharePointLocation.filePath.stripPrefix("/")
            val updatedSystemMetadata: List[MetadataProperty] = systemMetadata.map {
              case mp @ MetadataProperty(_, value) =>
                val normalizedValue = value.stripPrefix("/")
                if (normalizedValue == normalizedTarget) mp.copy(propertyValue = normalizedTarget) else mp
              case mp => mp
            }
            val input = ClientSideMetadataInput(sharePointLocation.filePath, metadata.SHA256ClientSideChecksum, dateLastModified, metadata.Length, metadata.matchId)
            logger.info(s"Asset metadata successfully processed for: $objectKey")
            val completedTags = Map(ptAp.toString -> Completed.toString)
            s3Utils.addObjectTags(event.s3SourceBucket, event.objectKey, completedTags)
            AssetProcessingResult(Some(matchId), processingErrors = false, Some(input), updatedSystemMetadata, suppliedMetadata)
          }
        }
      )
  }

  private def sharePointLocationPathToFilePath(locationPath: String): SharePointLocationPath = {
    val pathComponents = locationPath.split("/")
    SharePointLocationPath(pathComponents(1), pathComponents(2), pathComponents(3), pathComponents.slice(1, pathComponents.length).mkString("/"))
  }

  private def toMetadataProperties(json: Json, properties: Seq[String]): List[MetadataProperty] = {
    for {
      obj <- json.asObject.toList
      key <- properties
      value <- obj(key).flatMap(_.asString)
    } yield MetadataProperty(key, value)
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
  case class MetadataProperty(propertyName: String, propertyValue: String)
  case class AssetProcessingResult(
      matchId: Option[String],
      processingErrors: Boolean,
      clientSideMetadataInput: Option[ClientSideMetadataInput],
      systemMetadata: List[MetadataProperty] = List(),
      suppliedMetadata: List[MetadataProperty] = List()
  )
  case class AssetProcessingError(consignmentId: Option[String], matchId: Option[String], source: Option[String], errorCode: String, errorMsg: String) extends BaseError {
    override def toString: String = {
      s"${this.simpleName}: consignmentId: $consignmentId, matchId: $matchId, source: $source, errorCode: $errorCode, errorMessage: $errorMsg"
    }
  }
  case class SharePointLocationPath(root: String, site: String, library: String, filePath: String)

  val logger: Logger = Logger[AssetProcessing]

  private val configFactory: Config = ConfigFactory.load()
  val s3Utils: S3Utils = S3Utils(S3Clients.s3Async(configFactory.getString("s3.endpoint")))
  def apply() = new AssetProcessing(s3Utils)(logger)
}
