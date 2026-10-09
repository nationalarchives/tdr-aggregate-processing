package uk.gov.nationalarchives.aggregate.processing.modules.assetprocessing.initialchecks

import graphql.codegen.types
import uk.gov.nationalarchives.aggregate.processing.config.ApplicationConfig
import uk.gov.nationalarchives.aggregate.processing.modules.Common.ProcessErrorType.FileNameError
import uk.gov.nationalarchives.aggregate.processing.modules.Common.ProcessErrorValue.Invalid
import uk.gov.nationalarchives.aggregate.processing.modules.Common.ProcessType.InitialChecks
import uk.gov.nationalarchives.aggregate.processing.modules.assetprocessing.AssetProcessing
import uk.gov.nationalarchives.aggregate.processing.modules.assetprocessing.AssetProcessing.AssetProcessingError
import uk.gov.nationalarchives.tdr.schema.generated.ExcludedFilenames

class InvalidFileName(blockInvalidFileNameCheck: Boolean) extends InitialCheck {
  private val errorCode = s"$InitialChecks.$FileNameError.$Invalid"

  override val errorCodes: Set[String] = Set(errorCode)

  override def runCheck(event: AssetProcessing.AssetProcessingEvent, input: types.ClientSideMetadataInput): List[AssetProcessing.AssetProcessingError] = {
    if (blockInvalidFileNameCheck) {
      Nil
    } else {
      val fileName = input.originalPath.split("/").last
      if (ExcludedFilenames.isExcluded(fileName)) {
        val errorMessage = s"Invalid file name: $fileName"
        List(AssetProcessingError(Some(event.consignmentId), Some(event.matchId), Some(event.source.id), errorCode, errorMessage))
      } else {
        Nil
      }
    }
  }
}

object InvalidFileName {
  private val blockFeature = ApplicationConfig.blockInvalidFileNameCheck
  def apply() = new InvalidFileName(blockFeature)
}
