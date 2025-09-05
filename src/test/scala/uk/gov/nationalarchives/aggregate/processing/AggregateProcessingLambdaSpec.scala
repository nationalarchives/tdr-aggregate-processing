package uk.gov.nationalarchives.aggregate.processing

import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage
import com.github.tomakehurst.wiremock.client.WireMock.{anyUrl, getRequestedFor}
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class AggregateProcessingLambdaSpec extends ExternalServiceSpec {
  "process" should "process a valid event" in {
    mockS3GetResponse()
    val messageBody = s"""
    {
      "metadataSourceBucket": "source-bucket",
      "metadataSourceObjectPrefix": "/source/bucket/object/prefix"
    }
    """.stripMargin

    val message = new SQSMessage()
    message.setBody(messageBody)
    new AggregateProcessingLambda().process(message)
    wiremockS3.verify(
      getRequestedFor(anyUrl())
        .withUrl("/?list-type=2&max-keys=1000&prefix=%2Fsource%2Fbucket%2Fobject%2Fprefix")
    )
  }

  "process" should "throw an error for invalid message json" in {
    val nonJsonMessage = "some string"

    val message = new SQSMessage()
    message.setBody(nonJsonMessage)
    val exception = intercept[IllegalArgumentException] {
      new AggregateProcessingLambda().process(message)
    }
    exception.getMessage shouldBe "Invalid JSON object: expected json value got 'some s...' (line 1, column 1)"
  }

  "process" should "throw an error for invalid event" in {
    val invalidEventMessage = s"""
    {
      "property": "value"
    }
    """.stripMargin

    val message = new SQSMessage()
    message.setBody(invalidEventMessage)
    val exception = intercept[IllegalArgumentException] {
      new AggregateProcessingLambda().process(message)
    }
    exception.getMessage shouldBe "Invalid event: DecodingFailure at .metadataSourceBucket: Missing required field"
  }
}
