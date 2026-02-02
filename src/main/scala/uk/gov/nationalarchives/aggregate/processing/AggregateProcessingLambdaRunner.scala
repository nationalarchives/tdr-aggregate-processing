package uk.gov.nationalarchives.aggregate.processing

import cats.effect.unsafe.implicits.global
import uk.gov.nationalarchives.aggregate.processing.AggregateProcessingLambda.AggregateEvent

object AggregateProcessingLambdaRunner extends App {
  val event = AggregateEvent("tdr-upload-files-cloudfront-dirty-staging", "7ce3cd87-66b9-4d9e-aa38-183f4d84fa0c/sharepoint/921f62b5-fe40-4704-87c1-771c79e18fac/metadata", dataLoadErrors = false)

  new AggregateProcessingLambda().processEvent(event).unsafeRunSync()
}
