package uk.gov.nationalarchives.aggregate.processing.modules.assetprocessing.initialchecks

import graphql.codegen.types.ClientSideMetadataInput
import uk.gov.nationalarchives.aggregate.processing.config.ApplicationConfig.maxIndividualFileSizeMb
import uk.gov.nationalarchives.aggregate.processing.modules.Common.ProcessErrorType.ObjectSizeError
import uk.gov.nationalarchives.aggregate.processing.modules.Common.ProcessErrorValue.{TooBigError, TooSmallError}
import uk.gov.nationalarchives.aggregate.processing.modules.Common.ProcessType.InitialChecks
import uk.gov.nationalarchives.aggregate.processing.modules.assetprocessing.AssetProcessing.{AssetProcessingError, AssetProcessingEvent}

class FileSizeCheck extends InitialCheck {
  private val maxIndividualFileSizeBytes = maxIndividualFileSizeMb * 1000000L

  private def error(errorCode: String, fileSize: Long, event: AssetProcessingEvent): AssetProcessingError = {
    val transferId = event.consignmentId
    val source = event.source.toString
    val matchId = event.matchId
    val errorMsg = s"File size: $fileSize bytes"
    AssetProcessingError(Some(transferId), Some(matchId), Some(source), errorCode, errorMsg)
  }

  def runCheck(event: AssetProcessingEvent, input: ClientSideMetadataInput): List[AssetProcessingError] = {
    input.fileSize match {
      case fs if fs == 0 =>
        List(error(s"$InitialChecks.$ObjectSizeError.$TooSmallError", fs, event))
      case fs if fs > maxIndividualFileSizeBytes =>
        List(error(s"$InitialChecks.$ObjectSizeError.$TooBigError", fs, event))
      case _ => List()
    }
  }
}

object FileSizeCheck {
  def apply() = new FileSizeCheck()
}
