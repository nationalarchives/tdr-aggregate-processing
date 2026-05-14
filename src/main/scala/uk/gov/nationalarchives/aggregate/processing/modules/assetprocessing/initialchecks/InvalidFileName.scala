package uk.gov.nationalarchives.aggregate.processing.modules.assetprocessing.initialchecks

import graphql.codegen.types
import uk.gov.nationalarchives.aggregate.processing.modules.Common.ProcessErrorType.FileNameError
import uk.gov.nationalarchives.aggregate.processing.modules.Common.ProcessErrorValue.Invalid
import uk.gov.nationalarchives.aggregate.processing.modules.Common.ProcessType.InitialChecks
import uk.gov.nationalarchives.aggregate.processing.modules.assetprocessing.AssetProcessing
import uk.gov.nationalarchives.aggregate.processing.modules.assetprocessing.AssetProcessing.AssetProcessingError

class InvalidFileName extends InitialCheck {
  private val errorCode = s"$InitialChecks.$FileNameError.$Invalid"

  override val errorCodes: Set[String] = Set(errorCode)

  override def runCheck(event: AssetProcessing.AssetProcessingEvent, input: types.ClientSideMetadataInput): List[AssetProcessing.AssetProcessingError] = {
    val fileName = input.originalPath.split("/").last
    //TODO: use config to construct regex to identify invalid file name
    if (fileName == "thumbs.db") {
      val errorMessage = s"Invalid file name: $fileName"
      List(AssetProcessingError(Some(event.consignmentId), Some(event.matchId), Some(event.source.toString), errorCode, errorMessage))
    } else {
      Nil
    }
  }
}

object InvalidFileName {
  def apply() = new InvalidFileName()
}
