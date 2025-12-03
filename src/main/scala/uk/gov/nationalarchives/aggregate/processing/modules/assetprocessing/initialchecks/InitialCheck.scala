package uk.gov.nationalarchives.aggregate.processing.modules.assetprocessing.initialchecks

import graphql.codegen.types.ClientSideMetadataInput
import uk.gov.nationalarchives.aggregate.processing.modules.assetprocessing.AssetProcessing.{AssetProcessingError, AssetProcessingEvent}

trait InitialCheck {
  def runCheck(event: AssetProcessingEvent, input: ClientSideMetadataInput): List[AssetProcessingError]
}
