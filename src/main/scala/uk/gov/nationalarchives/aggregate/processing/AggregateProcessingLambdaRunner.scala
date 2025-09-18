package uk.gov.nationalarchives.aggregate.processing

import cats.effect.unsafe.implicits.global
import uk.gov.nationalarchives.aggregate.processing.AggregateProcessingLambda.AggregateEvent

object AggregateProcessingLambdaRunner extends App {
  val event = AggregateEvent("placeholder", "placeholder")

  new AggregateProcessingLambda().processEvent(event).unsafeRunSync()
}
