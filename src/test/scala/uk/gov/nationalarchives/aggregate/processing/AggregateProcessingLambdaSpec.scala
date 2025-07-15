package uk.gov.nationalarchives.aggregate.processing

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class AggregateProcessingLambdaSpec extends AnyFlatSpec {
  "processDataLoad" should "return 'Hello world' string" in {
    val result = new AggregateProcessingLambda().processDataLoad()
    result shouldBe "Hello world"
  }
}
