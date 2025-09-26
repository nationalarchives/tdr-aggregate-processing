package uk.gov.nationalarchives.aggregate.processing

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.events.SQSEvent
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage
import com.github.tomakehurst.wiremock.client.WireMock._
import org.mockito.MockitoSugar.mock
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import uk.gov.nationalarchives.aggregate.processing.modules.Common

import java.util.UUID
import scala.jdk.CollectionConverters._

class AggregateProcessingLambdaSpec extends ExternalServiceSpec {
  private val userId: UUID = UUID.randomUUID()
  private val consignmentId: UUID = UUID.randomUUID()
  private val matchId = "match-id"
  private val source: String = Common.AssetSource.SharePoint.toString
  private val category: String = Common.ObjectCategory.Metadata.toString
  private val validMessageBody: String =
    s"""
    {
      "metadataSourceBucket": "source-bucket",
      "metadataSourceObjectPrefix": "$userId/$source/$consignmentId/$category"
    }
    """.stripMargin

  "handleRequest" should "process all valid messages in SQS event" in {
    authOkJson()
    mockS3GetObjectStream(userId, consignmentId.toString, matchId)
    mockS3ListBucketResponse(userId, consignmentId, List(matchId))
    mockSfnResponseOk()
    mockGraphQlAddFilesAndMetadataResponse
    mockGraphQlUpdateConsignmentStatusResponse
    mockGraphQlGetConsignmentDetailsResponse
    val mockContext = mock[Context]

    val message1 = new SQSMessage()
    message1.setBody(validMessageBody)
    val message2 = new SQSMessage()
    message2.setBody(validMessageBody)
    val messages: java.util.List[SQSMessage] = List(message1, message2).asJava
    val sqsEvent = new SQSEvent()
    sqsEvent.setRecords(messages)
    new AggregateProcessingLambda().handleRequest(sqsEvent, mockContext)

    wiremockS3.verify(
      exactly(2),
      getRequestedFor(anyUrl())
        .withUrl(s"/?list-type=2&max-keys=1000&prefix=$userId%2F$source%2F$consignmentId%2F$category")
    )

    wiremockS3.verify(
      exactly(2),
      getRequestedFor(anyUrl())
        .withUrl(s"/$userId/$source/$consignmentId/$category/$matchId.metadata")
    )

    wiremockSfnServer.verify(
      exactly(2),
      postRequestedFor(anyUrl())
        .withRequestBody(containing(s"transfer_service_$consignmentId"))
    )

    wiremockGraphqlServer.verify(
      exactly(6),
      postRequestedFor(anyUrl())
        .withUrl("/graphql")
    )
  }

  "handleRequest" should "process request correctly when no objects are found for supplied object prefix" in {
    authOkJson()
    mockS3GetObjectStream(userId, consignmentId.toString, matchId)
    mockS3ListBucketResponseEmpty(userId, consignmentId)
    mockSfnResponseOk()
    mockGraphQlAddFilesAndMetadataResponse
    mockGraphQlUpdateConsignmentStatusResponse
    mockGraphQlGetConsignmentDetailsResponse
    val mockContext = mock[Context]

    val message1 = new SQSMessage()
    message1.setBody(validMessageBody)

    val messages: java.util.List[SQSMessage] = List(message1).asJava
    val sqsEvent = new SQSEvent()
    sqsEvent.setRecords(messages)
    new AggregateProcessingLambda().handleRequest(sqsEvent, mockContext)

    wiremockS3.verify(
      exactly(1),
      getRequestedFor(anyUrl())
        .withUrl(s"/?list-type=2&max-keys=1000&prefix=$userId%2F$source%2F$consignmentId%2F$category")
    )

    wiremockS3.verify(
      exactly(0),
      getRequestedFor(anyUrl())
        .withUrl(s"/$userId/$source/$consignmentId/$category/$matchId.metadata")
    )

    wiremockSfnServer.verify(
      exactly(0),
      postRequestedFor(anyUrl())
        .withRequestBody(containing(s"transfer_service_$consignmentId"))
    )

    wiremockGraphqlServer.verify(
      exactly(2),
      postRequestedFor(anyUrl())
        .withUrl("/graphql")
    )
  }

  "handleRequest" should "throw an error for invalid message json" in {
    val nonJsonMessage = "some string"
    val mockContext = mock[Context]

    val message = new SQSMessage()
    message.setBody(nonJsonMessage)
    val sqsEvent = new SQSEvent()
    sqsEvent.setRecords(List(message).asJava)
    val exception = intercept[IllegalArgumentException] {
      new AggregateProcessingLambda().handleRequest(sqsEvent, mockContext)
    }
    exception.getMessage shouldBe "Invalid JSON object: expected json value got 'some s...' (line 1, column 1)"

    wiremockS3.verify(
      exactly(0),
      getRequestedFor(anyUrl())
        .withUrl(s"/?list-type=2&max-keys=1000&prefix=$userId%2F$source%2F$consignmentId%2F$category")
    )

    wiremockS3.verify(
      exactly(0),
      getRequestedFor(anyUrl())
        .withUrl(s"/$userId/$source/$consignmentId/$category/$matchId.metadata")
    )

    wiremockSfnServer.verify(
      exactly(0),
      postRequestedFor(anyUrl())
        .withRequestBody(containing(s"transfer_service_$consignmentId"))
    )

    wiremockGraphqlServer.verify(
      exactly(0),
      postRequestedFor(anyUrl())
        .withUrl("/graphql")
    )
  }

  "handleRequest" should "throw an error for invalid event" in {
    val mockContext = mock[Context]
    val invalidEventMessage = s"""
      {
        "property": "value"
      }
      """.stripMargin

    val message = new SQSMessage()
    message.setBody(invalidEventMessage)
    val invalidEvent = new SQSEvent()
    invalidEvent.setRecords(List(message).asJava)
    val exception = intercept[IllegalArgumentException] {
      new AggregateProcessingLambda().handleRequest(invalidEvent, mockContext)
    }
    exception.getMessage shouldBe "Invalid event: DecodingFailure at .metadataSourceBucket: Missing required field"

    wiremockS3.verify(
      exactly(0),
      getRequestedFor(anyUrl())
        .withUrl(s"/?list-type=2&max-keys=1000&prefix=$userId%2F$source%2F$consignmentId%2F$category")
    )

    wiremockS3.verify(
      exactly(0),
      getRequestedFor(anyUrl())
        .withUrl(s"/$userId/$source/$consignmentId/$category/$matchId.metadata")
    )

    wiremockSfnServer.verify(
      exactly(0),
      postRequestedFor(anyUrl())
        .withRequestBody(containing(s"transfer_service_$consignmentId"))
    )

    wiremockGraphqlServer.verify(
      exactly(0),
      postRequestedFor(anyUrl())
        .withUrl("/graphql")
    )
  }
}
