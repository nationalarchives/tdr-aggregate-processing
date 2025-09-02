package uk.gov.nationalarchives.aggregate.processing.modules

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.Logger
import uk.gov.nationalarchives.aggregate.processing.modules.AssetProcessing.{AssetProcessingError, AssetProcessingResult, logger}
import uk.gov.nationalarchives.aggregate.processing.modules.ErrorHandling.BaseError
import uk.gov.nationalarchives.aggregate.processing.utilities.UTF8ValidationHandler
import uk.gov.nationalarchives.aws.utils.s3.{S3Clients, S3Utils}
import uk.gov.nationalarchives.utf8.validator.Utf8Validator

import scala.util.{Failure, Success, Try}

class AssetProcessing(s3Utils: S3Utils)(implicit logger: Logger) {
  //  Class to handle the processing of the metadata JSON sidecar
  def processAsset(s3Bucket: String, objectKey: String): Map[String, AssetProcessingResult] = {
    logger.info(s"Processing asset metadata for: $objectKey")
    getObjectAsStream(s3Utils, s3Bucket, objectKey) match {
      case Some(inputStream) =>
        validUTF8(inputStream, objectKey)
      case None => ()
    }
    /* TODO:
     *  - convert to Json object
     *  - check if contains and TDR 'supplied' fields, if so create 'SuppliedMetadata' case classes for each value
     *  - create 'ClientSideMetadataInput' Consignment API case class
     *  - write error Json to S3 if appropriate
     *  - return Map of matchId -> case class ('ClientSideMetadataInput', List[SuppliedMetadata], ProcessingErrors)
     */

    Map()
  }

  private def getObjectAsStream(s3Utils: S3Utils, s3Bucket: String, objectKey: String): Option[java.io.InputStream] = {
    Try(s3Utils.getObjectAsStream(s3Bucket, objectKey)) match {
      case Failure(ex) =>
        val s3ReadError = generateErrorMessage(objectKey, "ASSET_PROCESSING.S3.READ_ERROR", ex)
        ErrorHandling.handleError(s3ReadError, logger)
        None
      case Success(inputStream) => Some(inputStream)
    }
  }

  private def validUTF8(inputStream: java.io.InputStream, objectKey: String): Unit = {
    val utf8Validator = new Utf8Validator(new UTF8ValidationHandler()(logger))
    Try(utf8Validator.validate(inputStream)) match {
      case Failure(ex) =>
        val utf8Error = generateErrorMessage(objectKey, "ASSET_PROCESSING.UTF.INVALID", ex)
        ErrorHandling.handleError(utf8Error, logger)
      case _ => logger.info(s"UTF8 validation passed: $objectKey")
    }
  }

  private def generateErrorMessage(objectKey: String, errorCode: String, errorMsg: Throwable) = {
    val consignmentId = objectKey.split('/')(2)
    val matchId = objectKey.split('/').last.stripSuffix(".metadata")
    val source = objectKey.split('/')(1)
    AssetProcessingError(
      consignmentId = consignmentId,
      matchId = Some(matchId),
      source = source,
      errorCode = errorCode,
      errorMsg = errorMsg.getMessage
    )
  }
}

object AssetProcessing {
  case class SuppliedMetadata(propertyName: String, propertyValue: String)
  case class AssetProcessingResult(matchId: String, processingErrors: Boolean = false, suppliedMetadata: List[SuppliedMetadata] = List())
  case class AssetProcessingError(consignmentId: String, matchId: Option[String], source: String, errorCode: String, errorMsg: String) extends BaseError {
    override def toString: String = {
      s"${this.simpleName}: consignmentId: $consignmentId, matchId: $matchId, source: $source, errorCode: $errorCode, errorMessage: $errorMsg"
    }
  }
  val logger: Logger = Logger[AssetProcessing]

  private val configFactory: Config = ConfigFactory.load()
  val s3Utils: S3Utils = S3Utils(S3Clients.s3Async(configFactory.getString("s3.endpoint")))
  def apply() = new AssetProcessing(s3Utils)(logger)
}
