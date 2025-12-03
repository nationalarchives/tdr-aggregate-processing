package uk.gov.nationalarchives.aggregate.processing.modules.assetprocessing.initialchecks

import graphql.codegen.types.ClientSideMetadataInput
import uk.gov.nationalarchives.aggregate.processing.modules.Common.ProcessErrorType.FileExtensionError
import uk.gov.nationalarchives.aggregate.processing.modules.Common.ProcessErrorValue.Disallowed
import uk.gov.nationalarchives.aggregate.processing.modules.Common.ProcessType.InitialChecks
import uk.gov.nationalarchives.aggregate.processing.modules.assetprocessing.AssetProcessing.{AssetProcessingError, AssetProcessingEvent}

//TODO: need list of suspect file extensions from config. See: TDRD-913
class FileExtensionCheck extends InitialCheck {
  private lazy val errorCode = s"$InitialChecks.$FileExtensionError.$Disallowed"
  private def isJudgment(s3SourceBucket: String) = s3SourceBucket.contains("judgment")

  private def extensionNotAllowed(extension: String, sourceBucket: String): Boolean = {
    if (isJudgment(sourceBucket)) { false }
    else { false }
  }

  def runCheck(event: AssetProcessingEvent, input: ClientSideMetadataInput): List[AssetProcessingError] = {
    val extension = input.originalPath.split("\\.").last
    val sourceBucket = event.s3SourceBucket
    if (extensionNotAllowed(extension, sourceBucket)) {
      val transferId = event.consignmentId.toString
      val source = event.source.toString
      val matchId = event.matchId
      val errorMsg = s"File extension indicates disallowed file type: $extension"
      List(AssetProcessingError(Some(transferId), Some(matchId), Some(source), errorCode, errorMsg))
    } else {
      List()
    }
  }
}

object FileExtensionCheck {
  def apply() = new FileExtensionCheck
}
