package uk.gov.nationalarchives.aggregate.processing.modules.assetprocessing.initialchecks

import graphql.codegen.types.ClientSideMetadataInput
import uk.gov.nationalarchives.aggregate.processing.modules.Common.ProcessErrorType.UploadError
import uk.gov.nationalarchives.aggregate.processing.modules.Common.ProcessErrorValue.FolderOnly
import uk.gov.nationalarchives.aggregate.processing.modules.Common.ProcessType.InitialChecks
import uk.gov.nationalarchives.aggregate.processing.modules.assetprocessing.AssetProcessing.{AssetProcessingError, AssetProcessingEvent}

class FolderOnlyCheck extends InitialCheck {
  private lazy val errorCode = s"$InitialChecks.$UploadError.$FolderOnly"

  override def runCheck(event: AssetProcessingEvent, input: ClientSideMetadataInput): List[AssetProcessingError] = {
    val extension = input.originalPath.contains(".")
    if (!extension) {
      val transferId = event.consignmentId
      val source = event.source.toString
      val matchId = event.matchId
      val errorMsg = s"Empty folder uploaded"
      List(AssetProcessingError(Some(transferId), Some(matchId), Some(source), errorCode, errorMsg))
    } else Nil
  }
}

object FolderOnlyCheck {
  def apply() = new FolderOnlyCheck
}
