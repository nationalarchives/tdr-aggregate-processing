package uk.gov.nationalarchives.aggregate.processing

import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage

object AggregateProcessingLambdaRunner extends App {
  val messageBody = s"""
    {
      "metadataSourceBucket": "source-bucket",
      "metadataSourceObjectPrefix": "source-bucket-object-prefix"
    }
    """.stripMargin

  val sqsMessage = new SQSMessage
  sqsMessage.setBody(messageBody)

  new AggregateProcessingLambda().process(sqsMessage)
}
