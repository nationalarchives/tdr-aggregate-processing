package uk.gov.nationalarchives.aggregate.processing.modules.assetprocessing.initialchecks

import graphql.codegen.types.ClientSideMetadataInput
import uk.gov.nationalarchives.aggregate.processing.config.ApplicationConfig.maxIndividualFileSizeMb
import uk.gov.nationalarchives.aggregate.processing.modules.Common.ProcessErrorType.ObjectSizeError
import uk.gov.nationalarchives.aggregate.processing.modules.Common.ProcessErrorValue.{TooBigError, TooSmallError}
import uk.gov.nationalarchives.aggregate.processing.modules.Common.ProcessType.InitialChecks
import uk.gov.nationalarchives.aggregate.processing.modules.assetprocessing.AssetProcessing.{AssetProcessingError, AssetProcessingEvent}

class FileSizeCheck extends InitialCheck {
  private val maxIndividualFileSizeBytes = maxIndividualFileSizeMb * 1000000L
  private val fileTooSmallErrorCode = s"$InitialChecks.$ObjectSizeError.$TooSmallError"
  private val fileTooBigErrorCode = s"$InitialChecks.$ObjectSizeError.$TooBigError"

  override val errorCodes: Set[String] = Set(fileTooSmallErrorCode, fileTooBigErrorCode)

  private def error(errorCode: String, fileSize: Long, event: AssetProcessingEvent): AssetProcessingError = {
    val transferId = event.consignmentId
    val source = event.source.id
    val matchId = event.matchId
    val errorMsg = s"File size: $fileSize bytes"
    AssetProcessingError(Some(transferId), Some(matchId), Some(source), errorCode, errorMsg)
  }

  def runCheck(event: AssetProcessingEvent, input: ClientSideMetadataInput): List[AssetProcessingError] = {
    input.fileSize match {
      case fs if fs == 0 =>
        List(error(fileTooSmallErrorCode, fs, event))
      case fs if fs > maxIndividualFileSizeBytes =>
        List(error(fileTooBigErrorCode, fs, event))
      case _ => List()
    }
  }
}

object FileSizeCheck {
  def apply() = new FileSizeCheck()
}
