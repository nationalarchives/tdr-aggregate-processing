package uk.gov.nationalarchives.aggregate.processing

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.events.SQSEvent
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage
import com.github.tomakehurst.wiremock.client.WireMock._
import org.mockito.MockitoSugar.mock
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor1, TableFor2, TableFor3}
import uk.gov.nationalarchives.aggregate.processing.modules.Common
import uk.gov.nationalarchives.aggregate.processing.modules.Common.AssetSource

import java.util.UUID
import scala.jdk.CollectionConverters._

class AggregateProcessingLambdaSpec extends ExternalServiceSpec with TableDrivenPropertyChecks with MetadataHelper {
  private val userId: UUID = UUID.randomUUID()
  private val consignmentId: UUID = UUID.randomUUID()
  private val matchId = "match-id"
  private val source: String = Common.AssetSource.SharePoint.toString
  private val category: String = Common.ObjectCategory.Metadata.toString
  private def validMessageBody(assetSource: String): String =
    s"""
    {
      "metadataSourceBucket": "source-bucket",
      "metadataSourceObjectPrefix": "$userId/$assetSource/$consignmentId/$category",
      "dataLoadErrors": false
    }
    """.stripMargin

  val assetSources: TableFor3[String, (String, Long, UUID, Option[String], Option[String]) => String, String] = Table(
    ("Asset Source", "Metadata Json String", "Expected File Path"),
    (AssetSource.HardDrive.toString.toLowerCase, hardDriveMetadataJsonString, "content/Retail/Shared Documents/file1.txt"),
    (AssetSource.NetworkDrive.toString.toLowerCase, networkDriveJsonString, ""),
    (AssetSource.SharePoint.toString.toLowerCase, sharePointMetadataJsonString, "")
  )

  forAll(assetSources) { (assetSource, metadataJsonString, expectedFilePath) =>
    val objectKey = s"$userId/$assetSource/$consignmentId/$category/$matchId.metadata"

    s"'handleRequest' with asset source $assetSource" should "process all valid messages in SQS event" in {
      val metadata = metadataJsonString(matchId, defaultFileSize, consignmentId, None, None)
      authOkJson()
      mockS3GetObjectTagging(objectKey)
      mockS3GetObjectStream(objectKey, metadata)
      mockS3ListBucketResponse(userId, consignmentId, List(matchId), assetSource)
      mockSfnResponseOk()
      mockGraphQlAddFilesAndMetadataResponse
      mockGraphQlUpdateConsignmentStatusResponse
      mockGraphQlGetConsignmentResponse
      mockGraphQlUpdateParentFolderResponse
      val mockContext = mock[Context]

      val message1 = new SQSMessage()
      message1.setBody(validMessageBody(assetSource))
      val message2 = new SQSMessage()
      message2.setBody(validMessageBody(assetSource))
      val messages: java.util.List[SQSMessage] = List(message1, message2).asJava
      val sqsEvent = new SQSEvent()
      sqsEvent.setRecords(messages)
      new AggregateProcessingLambda().handleRequest(sqsEvent, mockContext)

      wiremockS3.verify(
        exactly(2),
        getRequestedFor(anyUrl())
          .withUrl(s"/?list-type=2&max-keys=1000&prefix=$userId%2F$assetSource%2F$consignmentId%2F$category")
      )

      wiremockS3.verify(
        exactly(2),
        getRequestedFor(anyUrl())
          .withUrl(s"/$objectKey")
      )

      wiremockS3.verify(
        exactly(4),
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

    s"'handleRequest' with asset source $assetSource" should "process request correctly where client side data load errors are present" in {
      val metadata = metadataJsonString(matchId, defaultFileSize, consignmentId, None, None)
      authOkJson()
      mockS3GetObjectTagging(objectKey)
      mockS3GetObjectStream(objectKey, metadata)
      mockS3ListBucketResponse(userId, consignmentId, List(matchId), assetSource)
      mockSfnResponseOk()
      mockGraphQlAddFilesAndMetadataResponse
      mockGraphQlUpdateConsignmentStatusResponse
      val mockContext = mock[Context]

      val dataLoadErrorsMessageBody: String =
        s"""
          {
            "metadataSourceBucket": "source-bucket",
            "metadataSourceObjectPrefix": "$userId/$assetSource/$consignmentId/$category",
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
          .withUrl(s"/?list-type=2&max-keys=1000&prefix=$userId%2F$assetSource%2F$consignmentId%2F$category")
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

    s"'handleRequest' with asset source $assetSource" should "process request correctly when no objects are found for supplied object prefix" in {
      val metadata = metadataJsonString(matchId, defaultFileSize, consignmentId, None, None)
      authOkJson()
      mockS3GetObjectTagging(objectKey)
      mockS3GetObjectStream(objectKey, metadata)
      mockS3ListBucketResponseEmpty(userId, consignmentId, assetSource)
      mockSfnResponseOk()
      mockGraphQlAddFilesAndMetadataResponse
      mockGraphQlUpdateConsignmentStatusResponse
      mockGraphQlGetConsignmentResponse
      val mockContext = mock[Context]

      val message1 = new SQSMessage()
      message1.setBody(validMessageBody(assetSource))

      val messages: java.util.List[SQSMessage] = List(message1).asJava
      val sqsEvent = new SQSEvent()
      sqsEvent.setRecords(messages)
      new AggregateProcessingLambda().handleRequest(sqsEvent, mockContext)

      wiremockS3.verify(
        exactly(1),
        getRequestedFor(anyUrl())
          .withUrl(s"/?list-type=2&max-keys=1000&prefix=$userId%2F$assetSource%2F$consignmentId%2F$category")
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

    s"'handleRequest' with asset source $assetSource" should "throw an error for invalid message json" in {
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
          .withUrl(s"/?list-type=2&max-keys=1000&prefix=$userId%2F$assetSource%2F$consignmentId%2F$category")
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

    s"'handleRequest' with asset source $assetSource" should "throw an error for invalid event" in {
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
          .withUrl(s"/?list-type=2&max-keys=1000&prefix=$userId%2F$assetSource%2F$consignmentId%2F$category")
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

    s"'handleRequest' with asset source $assetSource" should
      "upload a draft-metadata.csv to S3 when supplied metadata present in uploaded metadata" in {
        val metadata = metadataJsonString(matchId, defaultFileSize, consignmentId, Some(defaultSuppliedFields), None)
        authOkJson()
        mockS3GetObjectTagging(objectKey)
        mockS3GetObjectStream(objectKey, metadata)
        mockS3ListBucketResponse(userId, consignmentId, List(matchId), assetSource)
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
              "metadataSourceObjectPrefix": "$userId/$assetSource/$consignmentId/$category",
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
            .withRequestBody(containing(s"$expectedFilePath,file1.txt,2025-07-03,,some kind of description,,Open,,,,,No,,No,,English,,Crown copyright,,,"))
        )
      }
  }
}
