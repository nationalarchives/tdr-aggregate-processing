package uk.gov.nationalarchives.aggregate.processing.modules

import com.typesafe.scalalogging.Logger
import uk.gov.nationalarchives.aggregate.processing.modules.AssetProcessing.{AssetProcessingResult, logger}
import uk.gov.nationalarchives.aggregate.processing.modules.Source.Source

class AssetProcessing {
  //  Class to handle the processing of the metadata JSON sidecar
  def processAsset(s3Bucket: String, objectKey: String): Map[String, AssetProcessingResult] = {
    logger.info(s"Processing asset metadata for: $objectKey")
    /* TODO:
    *  - read s3 json object as input stream
    *  - UTF8 validation of input stream
    *  - convert to Json object
    *  - check if contains and TDR 'supplied' fields, if so create 'SuppliedMetadata' case classes for each value
    *  - create 'ClientSideMetadataInput' Consignment API case class
    *  - write error Json to S3 if appropriate
    *  - return Map of matchId -> case class ('ClientSideMetadataInput', List[SuppliedMetadata], ProcessingErrors)
    */
    Map()
  }
}

object AssetProcessing {
  case class SuppliedMetadata(propertyName: String, propertyValue: String)
  case class AssetProcessingResult(matchId: String,
                                   processingErrors: Boolean = false,
                                   suppliedMetadata: List[SuppliedMetadata] = List())
  case class AssetProcessingError(matchId: Option[String], source: Source, errorCode: String) extends ProcessingError
  val logger: Logger = Logger[AssetProcessing]
  def apply() = new AssetProcessing()
}
