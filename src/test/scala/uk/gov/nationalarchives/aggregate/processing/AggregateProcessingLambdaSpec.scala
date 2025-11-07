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
      "metadataSourceObjectPrefix": "$userId/$source/$consignmentId/$category",
      "dataLoadErrors": false
    }
    """.stripMargin

  "handleRequest" should "process all valid messages in SQS event" in {
    val objectKey = s"$userId/$source/$consignmentId/$category/$matchId.metadata"
    authOkJson()
    mockS3GetObjectTagging(objectKey)
    mockS3GetObjectStream(objectKey, consignmentId.toString, matchId)
    mockS3ListBucketResponse(userId, consignmentId, List(matchId))
    mockSfnResponseOk()
    mockGraphQlAddFilesAndMetadataResponse
    mockGraphQlUpdateConsignmentStatusResponse
    mockGraphQlGetConsignmentResponse
    mockGraphQlUpdateParentFolderResponse
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
        .withUrl(s"/$objectKey")
    )

    wiremockS3.verify(
      exactly(2),
      getRequestedFor(anyUrl())
        .withUrl(s"/$objectKey?tagging")
    )

    wiremockSfnServer.verify(
      exactly(2),
      postRequestedFor(anyUrl())
        .withRequestBody(containing(s"transfer_service_$consignmentId"))
    )

    wiremockGraphqlServer.verify(
      exactly(8),
      postRequestedFor(anyUrl())
        .withUrl("/graphql")
    )
  }

  "handleRequest" should "process request correctly where client side data load errors are present" in {
    val objectKey = s"$userId/$source/$consignmentId/$category/$matchId.metadata"
    authOkJson()
    mockS3GetObjectTagging(objectKey)
    mockS3GetObjectStream(objectKey, consignmentId.toString, matchId)
    mockS3ListBucketResponse(userId, consignmentId, List(matchId))
    mockSfnResponseOk()
    mockGraphQlAddFilesAndMetadataResponse
    mockGraphQlUpdateConsignmentStatusResponse
    val mockContext = mock[Context]

    val dataLoadErrorsMessageBody: String =
      s"""
    {
      "metadataSourceBucket": "source-bucket",
      "metadataSourceObjectPrefix": "$userId/$source/$consignmentId/$category",
      "dataLoadErrors": true
    }
    """.stripMargin

    val message = new SQSMessage()
    message.setBody(dataLoadErrorsMessageBody)

    val messages: java.util.List[SQSMessage] = List(message).asJava
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
        .withUrl(s"/$objectKey")
    )

    wiremockS3.verify(
      exactly(0),
      getRequestedFor(anyUrl())
        .withUrl(s"/$objectKey?tagging")
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

  "handleRequest" should "process request correctly when no objects are found for supplied object prefix" in {
    val objectKey = s"$userId/$source/$consignmentId/$category/$matchId.metadata"
    authOkJson()
    mockS3GetObjectTagging(objectKey)
    mockS3GetObjectStream(objectKey, consignmentId.toString, matchId)
    mockS3ListBucketResponseEmpty(userId, consignmentId)
    mockSfnResponseOk()
    mockGraphQlAddFilesAndMetadataResponse
    mockGraphQlUpdateConsignmentStatusResponse
    mockGraphQlGetConsignmentResponse
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
        .withUrl(s"/$objectKey")
    )

    wiremockS3.verify(
      exactly(0),
      getRequestedFor(anyUrl())
        .withUrl(s"/$objectKey?tagging")
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
    val objectKey = s"$userId/$source/$consignmentId/$category/$matchId.metadata"
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
        .withUrl(s"/$objectKey")
    )

    wiremockS3.verify(
      exactly(0),
      getRequestedFor(anyUrl())
        .withUrl(s"/$objectKey?tagging")
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
    val objectKey = s"$userId/$source/$consignmentId/$category/$matchId.metadata"
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
        .withUrl(s"/$objectKey")
    )

    wiremockS3.verify(
      exactly(0),
      getRequestedFor(anyUrl())
        .withUrl(s"/$objectKey?tagging")
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

  "handleRequest" should "upload a draft-metadata.csv to S3 when supplied metadata present in uploaded metadata" in {
    val objectKey = s"$userId/$source/$consignmentId/$category/$matchId.metadata"
    authOkJson()
    mockS3GetObjectTagging(objectKey)
    mockS3GetObjectStream(objectKey, consignmentId.toString, matchId, suppliedMetadata = true)
    mockS3ListBucketResponse(userId, consignmentId, List(matchId))
    mockSfnResponseOk()
    mockGraphQlAddFilesAndMetadataResponse
    mockGraphQlUpdateConsignmentStatusResponse
    mockGraphQlGetConsignmentResponse
    mockGraphQlUpdateParentFolderResponse

    val mockContext = mock[Context]

    val validMessageBody: String =
      s"""
      {
        "metadataSourceBucket": "source-bucket",
        "metadataSourceObjectPrefix": "$userId/$source/$consignmentId/$category",
        "dataLoadErrors": false
      }
      """.stripMargin

    val message = new SQSMessage()
    message.setBody(validMessageBody)
    val messages: java.util.List[SQSMessage] = List(message).asJava
    val sqsEvent = new SQSEvent()
    sqsEvent.setRecords(messages)

    new AggregateProcessingLambda().handleRequest(sqsEvent, mockContext)

    wiremockS3.verify(
      exactly(1),
      putRequestedFor(anyUrl())
        .withUrl(s"/draftMetadataBucket/$consignmentId/draft-metadata.csv")
        .withRequestBody(
          containing(
            "filepath,filename,date last modified,date of the record,description,former reference,closure status,closure start date,closure period,foi exemption code,foi schedule date,is filename closed,alternate filename,is description closed,alternate description,language,translated filename,copyright,related material,restrictions on use,evidence provided by"
          )
        )
        .withRequestBody(containing("sites/Retail/Shared Documents/file1.txt,file1.txt,2025-07-03,,some kind of description,,Open,,,,,No,,No,,English,,legal copyright,,,"))
    )
  }
}
