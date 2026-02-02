package uk.gov.nationalarchives.aggregate.processing

import cats.effect.unsafe.implicits.global
import uk.gov.nationalarchives.aggregate.processing.AggregateProcessingLambda.AggregateEvent

object AggregateProcessingLambdaRunner extends App {
  val event = AggregateEvent("tdr-upload-files-cloudfront-dirty-intg", "54a5e6d5-37eb-4ed5-b159-6ec1a899530b/sharepoint/73f4899c-aac9-4235-bfc9-818ea1c7d1c7/metadata", dataLoadErrors = false)

  new AggregateProcessingLambda().processEvent(event).unsafeRunSync()
}
