package uk.gov.nationalarchives.aggregate.processing

import cats.effect.unsafe.implicits.global
import uk.gov.nationalarchives.aggregate.processing.AggregateProcessingLambda.AggregateEvent

object AggregateProcessingLambdaRunner extends App {
  val event = AggregateEvent("tdr-upload-files-cloudfront-dirty-staging", "e5c347a6-ca8b-433f-9d50-c40809a0cfd9/sharepoint/bde99d64-e39d-402c-a0f8-f3a61b0d5f99/metadata")

  new AggregateProcessingLambda().processEvent(event).unsafeRunSync()
}
