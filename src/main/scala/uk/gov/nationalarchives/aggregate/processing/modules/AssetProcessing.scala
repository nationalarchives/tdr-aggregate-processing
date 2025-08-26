package uk.gov.nationalarchives.aggregate.processing.modules

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.Logger
import uk.gov.nationalarchives.aws.utils.s3.{S3Clients, S3Utils}
import uk.gov.nationalarchives.aggregate.processing.modules.AssetProcessing.{AssetProcessingError, AssetProcessingResult, logger, s3Utils}
import uk.gov.nationalarchives.aggregate.processing.modules.ErrorHandling.BaseError
import uk.gov.nationalarchives.aggregate.processing.modules.Source.Source
import uk.gov.nationalarchives.aggregate.processing.utilities.UTF8ValidationHandler
import uk.gov.nationalarchives.utf8.validator.Utf8Validator

import scala.util.{Failure, Try}

class AssetProcessing {
  //  Class to handle the processing of the metadata JSON sidecar
  def processAsset(s3Bucket: String, objectKey: String): Map[String, AssetProcessingResult] = {
    logger.info(s"Processing asset metadata for: $objectKey")
//     TODO:
//      - read s3 json object as input stream
//      - Object keys to process will be in the following form: /{user id}/{sharepoint}/{consignment id}/metadata/{match id}.metadata
    val inputStream = s3Utils.getObjectAsStream(s3Bucket, objectKey)
    //      - UTF8 validation of input stream
    validUTF8(inputStream, s3Bucket, objectKey)
//      - convert to Json object
//      - check if contains and TDR 'supplied' fields, if so create 'SuppliedMetadata' case classes for each value
//      - create 'ClientSideMetadataInput' Consignment API case class
//      - write error Json to S3 if appropriate
//      - return Map of matchId -> case class ('ClientSideMetadataInput', List[SuppliedMetadata], ProcessingErrors)

    Map()
  }

  private def validUTF8(inputStream: java.io.InputStream, s3Bucket: String, objectKey: String): Unit = {
    val utf8Validator = new Utf8Validator(new UTF8ValidationHandler()(logger))
    Try(utf8Validator.validate(inputStream)) match {
      case Failure(ex) =>
        val consignmentId = objectKey.split('/')(2)
        val matchId = objectKey.split('/').last
        val source = objectKey.split('/')(1)
        val utf8Error = AssetProcessingError(
          consignmentId = consignmentId,
          matchId = Some(matchId),
          source = Source.withName(source),
          errorCode = "ASSET_PROCESSING.UTF.INVALID",
          errorMsg = ex.getMessage
        )
        ErrorHandling.handleError(utf8Error, logger)
        throw new RuntimeException(s"UTF8 validation failed for S3 object: s3://$s3Bucket/$objectKey", ex)
      case _ => logger.info("UTF8 validation passed")
    }
  }
}

object AssetProcessing {
  case class SuppliedMetadata(propertyName: String, propertyValue: String)
  case class AssetProcessingResult(matchId: String, processingErrors: Boolean = false, suppliedMetadata: List[SuppliedMetadata] = List())
  case class AssetProcessingError(consignmentId: String, matchId: Option[String], source: Source, errorCode: String, errorMsg: String) extends BaseError {
    override def toString: String = {
      s"${AssetProcessingError.getClass.getSimpleName}: consignmentId: $consignmentId, matchId: $matchId, source: $source, errorCode: $errorCode, errorMessage: $errorMsg"
    }
  }
  val logger: Logger = Logger[AssetProcessing]

  private val configFactory: Config = ConfigFactory.load()
  val s3Utils: S3Utils = S3Utils(S3Clients.s3Async(configFactory.getString("s3.endpoint")))
  def apply() = new AssetProcessing()
}
