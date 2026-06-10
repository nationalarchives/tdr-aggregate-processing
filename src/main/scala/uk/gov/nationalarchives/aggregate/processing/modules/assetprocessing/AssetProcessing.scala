package uk.gov.nationalarchives.aggregate.processing.modules.assetprocessing

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.Logger
import graphql.codegen.types.ClientSideMetadataInput
import io.circe.{Json, parser}
import uk.gov.nationalarchives.aggregate.processing.config.ApplicationConfig.{malwareScanKey, malwareScanThreatFound}
import uk.gov.nationalarchives.aggregate.processing.modules.Common.MetadataClassification
import uk.gov.nationalarchives.aggregate.processing.modules.Common.ProcessErrorType.{EncodingError, JsonError, MalwareScanError, MatchIdError, ObjectKeyError, S3Error => s3e}
import uk.gov.nationalarchives.aggregate.processing.modules.Common.ProcessErrorValue.{Invalid, Mismatch, ReadError, ThreatFound}
import uk.gov.nationalarchives.aggregate.processing.modules.Common.ProcessType.{AssetProcessing => ptAp}
import uk.gov.nationalarchives.aggregate.processing.modules.ErrorHandling.BaseError
import uk.gov.nationalarchives.aggregate.processing.modules._
import uk.gov.nationalarchives.aggregate.processing.modules.assetprocessing.AssetProcessing.{AssetProcessingError, AssetProcessingEvent, AssetProcessingResult}
import uk.gov.nationalarchives.aggregate.processing.modules.assetprocessing.initialchecks.{FileSizeCheck, FolderOnlyCheck, InitialCheck, InvalidFileName}
import uk.gov.nationalarchives.aggregate.processing.modules.assetprocessing.metadata._
import uk.gov.nationalarchives.aggregate.processing.utilities.UTF8ValidationHandler
import uk.gov.nationalarchives.aws.utils.s3.{S3Clients, S3Utils}
import uk.gov.nationalarchives.tdr.common.utils.objectkeycontext.AssetSources.{AssetSource, HardDrive, NetworkDrive, SharePoint}
import uk.gov.nationalarchives.tdr.common.utils.objectkeycontext.{Context, ObjectCategories, ObjectTypes}
import uk.gov.nationalarchives.tdr.common.utils.objectkeycontext.ObjectTypes.ObjectType
import uk.gov.nationalarchives.tdr.common.utils.statuses.StatusValues.{CompletedValue, CompletedWithIssuesValue}
import uk.gov.nationalarchives.utf8.validator.Utf8Validator

import java.util.UUID
import scala.util.{Failure, Success, Try}

class AssetProcessing(s3Utils: S3Utils)(implicit logger: Logger) {
  private val fileSizeCheck = FileSizeCheck.apply()
  private val folderOnlyCheck = FolderOnlyCheck.apply()
  private val invalidFileNameCheck = InvalidFileName.apply()
  private lazy val initialChecks: Set[InitialCheck] = Set(
    fileSizeCheck,
    folderOnlyCheck,
    invalidFileNameCheck
  )
  private lazy val errorHandling = ErrorHandling()

  private def getMetadataHandler(assetSource: AssetSource): MetadataHandler = {
    assetSource match {
      case HardDrive    => HardDriveMetadataHandler.metadataHandler
      case NetworkDrive => NetworkDriveMetadataHandler.metadataHandler
      case SharePoint   => SharePointMetadataHandler.metadataHandler
    }
  }

  private def handleProcessError(errors: List[AssetProcessingError], s3Bucket: String, s3ObjectKey: String): AssetProcessingResult = {
    errors.foreach(errorHandling.handleError(_, logger))
    val errorTags: Map[String, String] = Map(ptAp.toString -> CompletedWithIssuesValue.value)
    s3Utils.addObjectTags(s3Bucket, s3ObjectKey, errorTags)
    val matchId = errors.head.matchId
    AssetProcessingResult(matchId, processingErrors = true, None)
  }

  def processAsset(s3Bucket: String, objectKey: String, ignoreSiteName: Boolean): AssetProcessingResult = {
    Try {
      val objectContext = Context.objectKeyParser(objectKey, s3Bucket)
      val objectElements = objectContext.objectName.get.split("\\.")
      val matchId = objectElements(0)
      AssetProcessingEvent(
        objectContext.userId.get,
        objectContext.transferId.get,
        matchId,
        objectContext.assetSource.get,
        objectContext.objectType.get,
        s3Bucket,
        objectKey,
        ignoreSiteName
      )
    } match {
      case Failure(ex) =>
        val error = AssetProcessingError(None, None, None, s"$ptAp.$ObjectKeyError.$Invalid", s"${ex.getMessage}")
        handleProcessError(List(error), s3Bucket, objectKey)
      case Success(event) => parseMetadataObject(s3Utils, event)
    }
  }

  private def parseMetadataObject(s3Utils: S3Utils, event: AssetProcessingEvent): AssetProcessingResult = {
    val s3Bucket = event.s3SourceBucket
    val objectKey = event.objectKey
    val objectTags = s3Utils.getObjectTags(s3Bucket, objectKey)

    checkMalwareScan(objectTags, s3Bucket, objectKey, event) match {
      case Some(result) => result
      case None =>
        Try(s3Utils.getObjectAsStream(s3Bucket, objectKey)) match {
          case Failure(ex) =>
            val error = generateErrorMessage(event, s"$ptAp.$s3e.$ReadError", ex.getMessage)
            handleProcessError(List(error), s3Bucket, objectKey)
          case Success(inputStream) => validUTF8(inputStream, event)
        }
    }
  }

  private def checkMalwareScan(
      objectTags: Map[String, String],
      s3Bucket: String,
      objectKey: String,
      event: AssetProcessingEvent
  ): Option[AssetProcessingResult] = {
    objectTags.get(malwareScanKey) match {
      case Some(value) if value == malwareScanThreatFound =>
        val error = generateErrorMessage(event, s"$ptAp.$MalwareScanError.$ThreatFound", "malware scan threat found")
        Some(handleProcessError(List(error), s3Bucket, objectKey))
      case _ => None
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

  private val ignoreInitialChecksErrorCodes = invalidFileNameCheck.errorCodes

  private def handleInitialChecksErrors(event: AssetProcessingEvent, initialChecksErrors: List[AssetProcessingError], input: ClientSideMetadataInput) = {
    val s3Bucket = event.s3SourceBucket
    val objectKey = event.objectKey
    val matchId = event.matchId
    if (initialChecksErrors.forall(e => ignoreInitialChecksErrorCodes.contains(e.errorCode))) {
      val errorCodes = initialChecksErrors.map(_.errorCode).mkString(",")
      val logMessage = s"Asset ignored: $objectKey with error codes $errorCodes"
      logger.info(logMessage)
      val ignoreObjectTag = Map("IGNORE_OBJECT" -> "TRUE")
      val recordObjectKey = s"${event.userId}/${event.source.id}/${event.consignmentId}/${ObjectCategories.Records.id}/${event.matchId}"
      s3Utils.addObjectTags(s3Bucket, objectKey, ignoreObjectTag)
      s3Utils.addObjectTags(s3Bucket, recordObjectKey, ignoreObjectTag)
      AssetProcessingResult(Some(matchId), processingErrors = false, Some(input), ignoreAsset = true)
    } else {
      handleProcessError(initialChecksErrors, s3Bucket, objectKey)
      AssetProcessingResult(Some(matchId), processingErrors = true, Some(input))
    }
  }

  private def parseMetadataJson(sourceJson: Json, event: AssetProcessingEvent): AssetProcessingResult = {
    val metadataHandler: MetadataHandler = getMetadataHandler(event.source)
    val matchId = event.matchId
    val objectKey = event.objectKey
    val s3Bucket = event.s3SourceBucket
    val baseMetadataJson = metadataHandler.convertToBaseMetadata(sourceJson, Some(event.ignoreSiteName))
    metadataHandler
      .toClientSideMetadataInput(baseMetadataJson)
      .fold(
        err => {
          val error = AssetProcessingError(Some(event.consignmentId), Some(event.matchId), Some(event.source.id), s"$ptAp.$JsonError.$Invalid", err.getMessage())
          handleProcessError(List(error), s3Bucket, objectKey)
        },
        input => {
          if (event.matchId != input.matchId) {
            val error = AssetProcessingError(
              Some(event.consignmentId),
              None,
              Some(event.source.id),
              s"$ptAp.$MatchIdError.$Mismatch",
              s"Mismatched match ids: ${event.matchId} and ${input.matchId}"
            )
            handleProcessError(List(error), s3Bucket, objectKey)
          } else {
            val initialChecksErrors: List[AssetProcessingError] = initialChecks.flatMap(_.runCheck(event, input)).toList
            if (initialChecksErrors.nonEmpty) {
              handleInitialChecksErrors(event, initialChecksErrors, input)
            } else {
              val classifiedMetadata = metadataHandler.classifyBaseMetadata(baseMetadataJson)
              val suppliedMetadata = classifiedMetadata.getOrElse(MetadataClassification.Supplied, Nil)
              val systemMetadata = classifiedMetadata.getOrElse(MetadataClassification.System, Nil)
              val customMetadata = classifiedMetadata.getOrElse(MetadataClassification.Custom, Nil)
              logger.info(s"Asset metadata successfully processed for: $objectKey")
              val completedTags = Map(ptAp.toString -> CompletedValue.value)
              s3Utils.addObjectTags(event.s3SourceBucket, event.objectKey, completedTags)
              AssetProcessingResult(Some(matchId), processingErrors = false, Some(input), systemMetadata, suppliedMetadata, customMetadata)
            }
          }
        }
      )
  }

  private def generateErrorMessage(event: AssetProcessingEvent, errorCode: String, errorMessage: String): AssetProcessingError = {
    AssetProcessingError(
      consignmentId = Some(event.consignmentId),
      matchId = Some(event.matchId),
      source = Some(event.source.id),
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
      objectKey: String,
      ignoreSiteName: Boolean
  )

  case class AssetProcessingResult(
      matchId: Option[String],
      processingErrors: Boolean,
      clientSideMetadataInput: Option[ClientSideMetadataInput],
      systemMetadata: List[MetadataProperty] = List(),
      suppliedMetadata: List[MetadataProperty] = List(),
      customMetadata: List[MetadataProperty] = List(),
      ignoreAsset: Boolean = false
  )
  case class AssetProcessingError(consignmentId: Option[UUID], matchId: Option[String], source: Option[String], errorCode: String, errorMsg: String) extends BaseError {
    override def toString: String = {
      s"${this.simpleName}: consignmentId: $consignmentId, matchId: $matchId, source: $source, errorCode: $errorCode, errorMessage: $errorMsg"
    }
  }

  val logger: Logger = Logger[AssetProcessing]

  private val configFactory: Config = ConfigFactory.load()
  val s3Utils: S3Utils = S3Utils(S3Clients.s3Async(configFactory.getString("s3.endpoint")))
  def apply() = new AssetProcessing(s3Utils)(logger)
}
