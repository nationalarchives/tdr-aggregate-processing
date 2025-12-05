package uk.gov.nationalarchives.aggregate.processing.modules.assetprocessing

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.Logger
import graphql.codegen.types.ClientSideMetadataInput
import io.circe.{Json, parser}
import uk.gov.nationalarchives.aggregate.processing.modules.Common.AssetSource.AssetSource
import uk.gov.nationalarchives.aggregate.processing.modules.Common.ObjectType.ObjectType
import uk.gov.nationalarchives.aggregate.processing.modules.Common.ProcessErrorType.{EncodingError, JsonError, MatchIdError, ObjectKeyParsingError, S3Error => s3e}
import uk.gov.nationalarchives.aggregate.processing.modules.Common.ProcessErrorValue.{Invalid, Mismatch, ReadError}
import uk.gov.nationalarchives.aggregate.processing.modules.Common.ProcessType.{AssetProcessing => ptAp}
import uk.gov.nationalarchives.aggregate.processing.modules.Common.StateStatusValue.{Completed, CompletedWithIssues}
import uk.gov.nationalarchives.aggregate.processing.modules.Common.{AssetSource, ObjectType}
import uk.gov.nationalarchives.aggregate.processing.modules.ErrorHandling.BaseError
import uk.gov.nationalarchives.aggregate.processing.modules._
import uk.gov.nationalarchives.aggregate.processing.modules.assetprocessing.AssetProcessing.{AssetProcessingError, AssetProcessingEvent, AssetProcessingResult}
import uk.gov.nationalarchives.aggregate.processing.modules.assetprocessing.initialchecks.{FileExtensionCheck, FileSizeCheck, InitialCheck}
import uk.gov.nationalarchives.aggregate.processing.modules.assetprocessing.metadata._
import uk.gov.nationalarchives.aggregate.processing.utilities.UTF8ValidationHandler
import uk.gov.nationalarchives.aws.utils.s3.{S3Clients, S3Utils}
import uk.gov.nationalarchives.tdr.schemautils.ConfigUtils
import uk.gov.nationalarchives.utf8.validator.Utf8Validator

import java.util.UUID
import scala.util.{Failure, Success, Try}

class AssetProcessing(s3Utils: S3Utils)(implicit logger: Logger) {
  private lazy val metadataConfig: ConfigUtils.MetadataConfiguration = ConfigUtils.loadConfiguration
  private lazy val tdrDataLoadHeaderToPropertyMapper: String => String = metadataConfig.propertyToOutputMapper("tdrFileHeader")
  private lazy val initialChecks: Set[InitialCheck] = Set(FileSizeCheck.apply(), FileExtensionCheck.apply())

  private def getMetadataHandler(assetSource: AssetSource): MetadataHandler = {
    assetSource match {
      case AssetSource.HardDrive  => DroidMetadataHandler.metadataHandler
      case AssetSource.SharePoint => SharePointMetadataHandler.metadataHandler
    }
  }

  private def handleProcessError(errors: List[AssetProcessingError], s3Bucket: String, s3ObjectKey: String): AssetProcessingResult = {
    errors.foreach(ErrorHandling.handleError(_, logger))
    val errorTags: Map[String, String] = Map(ptAp.toString -> CompletedWithIssues.toString)
    s3Utils.addObjectTags(s3Bucket, s3ObjectKey, errorTags)
    val matchId = errors.head.matchId
    AssetProcessingResult(matchId, processingErrors = true, None)
  }

  def processAsset(s3Bucket: String, objectKey: String): AssetProcessingResult = {
    Try {
      val objectDetails = Common.objectKeyParser(objectKey)
      val objectElements = objectDetails.objectElements.get.split("\\.")
      val matchId = objectElements(0)
      val objectType = ObjectType.withName(objectElements(1))
      AssetProcessingEvent(objectDetails.userId, objectDetails.consignmentId, matchId, objectDetails.assetSource, objectType, s3Bucket, objectKey)
    } match {
      case Failure(ex) =>
        val error = AssetProcessingError(None, None, None, s"$ptAp.$ObjectKeyParsingError.$Invalid", s"Invalid object key: $objectKey: ${ex.getMessage}")
        handleProcessError(List(error), s3Bucket, objectKey)
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
        handleProcessError(List(error), s3Bucket, objectKey)
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
        handleProcessError(List(error), s3Bucket, objectKey)
      case Success(Left(parseEx)) =>
        val error = generateErrorMessage(event, s"$ptAp.$JsonError.$Invalid", parseEx.getMessage())
        handleProcessError(List(error), s3Bucket, objectKey)
      case Success(Right(json)) =>
        parseMetadataJson(json, event)
    }
  }

  private def parseMetadataJson(sourceJson: Json, event: AssetProcessingEvent): AssetProcessingResult = {
    val metadataHandler: MetadataHandler = getMetadataHandler(event.source)
    val matchId = event.matchId
    val objectKey = event.objectKey
    val s3Bucket = event.s3SourceBucket
    val suppliedProperties: Seq[String] = metadataConfig.getPropertiesByPropertyType("Supplied").map(p => tdrDataLoadHeaderToPropertyMapper(p))
    val systemProperties: Seq[String] = metadataConfig.getPropertiesByPropertyType("System")
    val baseMetadataJson = metadataHandler.convertToBaseMetadata(sourceJson)
    val allPropertyNames: Seq[String] = baseMetadataJson.asObject.map(_.keys.toSeq).getOrElse(Seq.empty)
    val excludeProperties = suppliedProperties ++ systemProperties :+ MatchIdProperty.id :+ TransferIdProperty.id
    val customProperties = allPropertyNames.diff(excludeProperties)
    metadataHandler
      .toClientSideMetadataInput(baseMetadataJson)
      .fold(
        err => {
          val error = AssetProcessingError(Some(event.consignmentId.toString), Some(event.matchId), Some(event.source.toString), s"$ptAp.$JsonError.$Invalid", err.getMessage())
          handleProcessError(List(error), s3Bucket, objectKey)
        },
        input => {
          if (event.matchId != input.matchId) {
            val error = AssetProcessingError(
              Some(event.consignmentId.toString),
              None,
              Some(event.source.toString),
              s"$ptAp.$MatchIdError.$Mismatch",
              s"Mismatched match ids: ${event.matchId} and ${input.matchId}"
            )
            handleProcessError(List(error), s3Bucket, objectKey)
          } else {
            val initialChecksErrors: List[AssetProcessingError] = initialChecks.flatMap(_.runCheck(event, input)).toList
            if (initialChecksErrors.nonEmpty) {
              handleProcessError(initialChecksErrors, s3Bucket, objectKey)
              AssetProcessingResult(Some(matchId), processingErrors = true, Some(input))
            } else {
              val suppliedMetadata = metadataHandler.toMetadataProperties(baseMetadataJson, suppliedProperties)
              val systemMetadata = metadataHandler.toMetadataProperties(baseMetadataJson, systemProperties)
              val customMetadata = metadataHandler.toMetadataProperties(baseMetadataJson, customProperties)
              logger.info(s"Asset metadata successfully processed for: $objectKey")
              val completedTags = Map(ptAp.toString -> Completed.toString)
              s3Utils.addObjectTags(event.s3SourceBucket, event.objectKey, completedTags)
              AssetProcessingResult(Some(matchId), processingErrors = false, Some(input), systemMetadata, suppliedMetadata, customMetadata)
            }
          }
        }
      )
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
  case class AssetProcessingEvent(
      userId: UUID,
      consignmentId: UUID,
      matchId: String,
      source: AssetSource,
      objectType: ObjectType,
      s3SourceBucket: String,
      objectKey: String
  )

  case class AssetProcessingResult(
      matchId: Option[String],
      processingErrors: Boolean,
      clientSideMetadataInput: Option[ClientSideMetadataInput],
      systemMetadata: List[MetadataProperty] = List(),
      suppliedMetadata: List[MetadataProperty] = List(),
      customMetadata: List[MetadataProperty] = List()
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
