package uk.gov.nationalarchives.aggregate.processing

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.events.SQSEvent
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.stubbing.StubMapping
import org.mockito.MockitoSugar.mock
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor4}
import uk.gov.nationalarchives.tdr.common.utils.objectkeycontext.AssetSources.{HardDrive, NetworkDrive, SharePoint}
import uk.gov.nationalarchives.tdr.common.utils.objectkeycontext.ObjectCategories.{Metadata, Records}
import uk.gov.nationalarchives.tdr.common.utils.statuses.StatusValues
import uk.gov.nationalarchives.tdr.common.utils.statuses.StatusValues.InProgressValue

import java.util.UUID
import scala.jdk.CollectionConverters._

class AggregateProcessingLambdaSpec extends ExternalServiceSpec with TableDrivenPropertyChecks with MetadataHelper {
  private val userId: UUID = UUID.randomUUID()
  private val consignmentId: UUID = UUID.randomUUID()
  private val matchId = "match-id"
  private val metadataCategory: String = Metadata.id
  private val recordsCategory: String = Records.id
  private def validMessageBody(assetSource: String): String =
    s"""
    {
      "metadataSourceBucket": "$metadataSourceBucket",
      "metadataSourceObjectPrefix": "$userId/$assetSource/$consignmentId/$metadataCategory",
      "recordsSourceBucket": "$recordsSourceBucket",
      "recordsSourceObjectPrefix": "$userId/$assetSource/$consignmentId/$recordsCategory",
      "dataLoadErrors": false,
      "ignoreSiteName": false,
      "loadedNumberOfFiles": 2
    }
    """.stripMargin

  def getMetadataObjectKey(assetSource: String) =
    s"$userId/$assetSource/$consignmentId/$metadataCategory/$matchId.metadata"

  def getRecordObjectKey(assetSource: String) =
    s"$userId/$assetSource/$consignmentId/records/$matchId"

  val assetSources: TableFor4[String, (String, Long, UUID, Option[String], Option[String]) => String, String, String] = Table(
    ("Asset Source", "Metadata Json String", "Expected File Path", "Invalid Supplied Metadata"),
    (
      HardDrive.id.toLowerCase,
      hardDriveMetadataJsonString,
      "content/Retail/Shared Documents/file1.txt",
      """"closure_type": "Open","description": "some kind of description","date_last_modified": "2025-13-45T25:99:99""""
    ),
    (
      NetworkDrive.id.toLowerCase,
      networkDriveJsonString,
      "",
      """"closure_type": "Open","description": "some kind of description","lastModified": "17/02/2026""""
    ),
    (
      SharePoint.id.toLowerCase,
      sharePointMetadataJsonString,
      "",
      """"closure_type": "Open","description": "some kind of description","date_x0020_of_x0020_the_x0020_record": "17/02/2026""""
    )
  )

  forAll(assetSources) { (assetSource, metadataJsonString, expectedFilePath, invalidSuppliedMetadata) =>
    val metadataObjectKey = getMetadataObjectKey(assetSource)
    val recordObjectKey = getRecordObjectKey(assetSource)

    s"'handleRequest' with asset source $assetSource" should "process all valid messages in SQS event" in {
      val metadata = metadataJsonString(matchId, defaultFileSize, consignmentId, None, None)
      mockBaseResponses(metadataObjectKey, metadata, assetSource, recordObjectKey)
      mockGraphQlGetConsignmentResponse()

      val mockContext = mock[Context]

      val message1 = new SQSMessage()
      message1.setBody(validMessageBody(assetSource))
      val message2 = new SQSMessage()
      message2.setBody(validMessageBody(assetSource))
      val messages: java.util.List[SQSMessage] = List(message1, message2).asJava
      val sqsEvent = new SQSEvent()
      sqsEvent.setRecords(messages)
      new AggregateProcessingLambda().handleRequest(sqsEvent, mockContext)

      validateGetObjectKeysRequests(numberOfRequests = 2, assetSource = assetSource)
      validateMetadataReadRequests(numberOfRequests = 2, metadataObjectKey = metadataObjectKey)
      validateTriggerStepFunctionRequests(numberOfRequests = 2)
      validatePublishSnsMessageRequests(numberOfRequests = 2)
      validatePersistenceRequests(10)
      validateErrorBucketPutRequests(numberOfErrors = 0)
      validateGetTaggingRequests(metadataObjectKey, 4, recordObjectKey, 0)
      validatePutTaggingRequests(metadataObjectKey, 2, recordObjectKey, 0, defaultPutTagsBodyXml())
    }

    s"'handleRequest' with asset source $assetSource" should "process all valid messages in SQS event correctly when transfer state incorrect" in {
      val metadata = metadataJsonString(matchId, defaultFileSize, consignmentId, None, None)
      val incorrectUploadStatusValue: StatusValues.StatusValue = InProgressValue
      mockBaseResponses(metadataObjectKey, metadata, assetSource, recordObjectKey)
      mockGraphQlGetConsignmentResponse(uploadStatusValue = incorrectUploadStatusValue)
      val mockContext = mock[Context]

      val message = new SQSMessage()
      message.setBody(validMessageBody(assetSource))

      val messages: java.util.List[SQSMessage] = List(message).asJava
      val sqsEvent = new SQSEvent()
      sqsEvent.setRecords(messages)
      new AggregateProcessingLambda().handleRequest(sqsEvent, mockContext)

      validateGetObjectKeysRequests(assetSource = assetSource)
      validateMetadataReadRequests(numberOfRequests = 0, metadataObjectKey = metadataObjectKey)
      validateNoObjectTagging(metadataObjectKey, recordObjectKey)
      validateTriggerStepFunctionRequests(numberOfRequests = 0)
      validatePersistenceRequests(3)
      validatePublishSnsMessageRequests()
      validateErrorBucketPutRequests(numberOfErrors = 2)
      validateGetTaggingRequests(metadataObjectKey, 0, recordObjectKey, 0)
      validatePutTaggingRequests(metadataObjectKey, 0, recordObjectKey, 0, defaultPutTagsBodyXml())
    }

    s"'handleRequest' with asset source $assetSource" should "process request correctly where client side data load errors are present" in {
      val metadata = metadataJsonString(matchId, defaultFileSize, consignmentId, None, None)
      mockBaseResponses(metadataObjectKey, metadata, assetSource, recordObjectKey)
      mockGraphQlGetConsignmentResponse()
      val mockContext = mock[Context]

      val dataLoadErrorsMessageBody: String =
        s"""
          {
            "metadataSourceBucket": "$metadataSourceBucket",
            "metadataSourceObjectPrefix": "$userId/$assetSource/$consignmentId/$metadataCategory",
            "recordsSourceBucket": "$recordsSourceBucket",
            "recordsSourceObjectPrefix": "$userId/$assetSource/$consignmentId/$recordsCategory",
            "dataLoadErrors": true,
            "ignoreSiteName": false,
            "loadedNumberOfFiles": 2
          }
          """.stripMargin

      val message = new SQSMessage()
      message.setBody(dataLoadErrorsMessageBody)

      val messages: java.util.List[SQSMessage] = List(message).asJava
      val sqsEvent = new SQSEvent()
      sqsEvent.setRecords(messages)
      new AggregateProcessingLambda().handleRequest(sqsEvent, mockContext)

      validateGetObjectKeysRequests(assetSource = assetSource)
      validateMetadataReadRequests(numberOfRequests = 0, metadataObjectKey = metadataObjectKey)
      validateNoObjectTagging(metadataObjectKey, recordObjectKey)
      validateTriggerStepFunctionRequests(numberOfRequests = 0)
      validatePersistenceRequests(3)
      validatePublishSnsMessageRequests()
      validateErrorBucketPutRequests(numberOfErrors = 2)
      validateGetTaggingRequests(metadataObjectKey, 0, recordObjectKey, 0)
      validatePutTaggingRequests(metadataObjectKey, 0, recordObjectKey, 0, defaultPutTagsBodyXml())
    }

    s"'handleRequest' with asset source $assetSource" should "process request correctly where there is an object count mis-match" in {
      val metadata = metadataJsonString(matchId, defaultFileSize, consignmentId, None, None)
      mockBaseResponses(metadataObjectKey, metadata, assetSource, recordObjectKey)
      mockGraphQlGetConsignmentResponse()
      val mockContext = mock[Context]

      val objectCountErrorsMessageBody: String =
        s"""
          {
            "metadataSourceBucket": "$metadataSourceBucket",
            "metadataSourceObjectPrefix": "$userId/$assetSource/$consignmentId/$metadataCategory",
            "recordsSourceBucket": "$recordsSourceBucket",
            "recordsSourceObjectPrefix": "$userId/$assetSource/$consignmentId/$recordsCategory",
            "dataLoadErrors": true,
            "ignoreSiteName": false,
            "loadedNumberOfFiles": 3
          }
          """.stripMargin

      val message = new SQSMessage()
      message.setBody(objectCountErrorsMessageBody)

      val messages: java.util.List[SQSMessage] = List(message).asJava
      val sqsEvent = new SQSEvent()
      sqsEvent.setRecords(messages)
      new AggregateProcessingLambda().handleRequest(sqsEvent, mockContext)

      validateGetObjectKeysRequests(assetSource = assetSource)
      validateMetadataReadRequests(numberOfRequests = 0, metadataObjectKey = metadataObjectKey)
      validateNoObjectTagging(metadataObjectKey, recordObjectKey)
      validateTriggerStepFunctionRequests(numberOfRequests = 0)
      validatePersistenceRequests(3)
      validatePublishSnsMessageRequests()
      validateErrorBucketPutRequests(numberOfErrors = 2)
      validateGetTaggingRequests(metadataObjectKey, 0, recordObjectKey, 0)
      validatePutTaggingRequests(metadataObjectKey, 0, recordObjectKey, 0, defaultPutTagsBodyXml())
    }

    s"'handleRequest' with asset source $assetSource" should "process request correctly when no objects are found for supplied object prefix" in {
      val metadata = metadataJsonString(matchId, defaultFileSize, consignmentId, None, None)
      mockBaseResponses(metadataObjectKey, metadata, assetSource, recordObjectKey)
      mockS3ListBucketResponseEmpty(userId, consignmentId, assetSource)
      mockGraphQlGetConsignmentResponse()
      val mockContext = mock[Context]

      val message1 = new SQSMessage()
      message1.setBody(validMessageBody(assetSource))

      val messages: java.util.List[SQSMessage] = List(message1).asJava
      val sqsEvent = new SQSEvent()
      sqsEvent.setRecords(messages)
      new AggregateProcessingLambda().handleRequest(sqsEvent, mockContext)

      validateGetObjectKeysRequests(assetSource = assetSource)
      validateMetadataReadRequests(numberOfRequests = 0, metadataObjectKey = metadataObjectKey)
      validateNoObjectTagging(metadataObjectKey, recordObjectKey)
      validateTriggerStepFunctionRequests(numberOfRequests = 0)
      validatePersistenceRequests(3)
      validatePublishSnsMessageRequests()
      validateErrorBucketPutRequests(numberOfErrors = 2)
      validateGetTaggingRequests(metadataObjectKey, 0, recordObjectKey, 0)
      validatePutTaggingRequests(metadataObjectKey, 0, recordObjectKey, 0, defaultPutTagsBodyXml())
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

      validateGetObjectKeysRequests(numberOfRequests = 0, assetSource = assetSource)
      validateMetadataReadRequests(numberOfRequests = 0, metadataObjectKey = metadataObjectKey)
      validateNoObjectTagging(metadataObjectKey, recordObjectKey)
      validateTriggerStepFunctionRequests(numberOfRequests = 0)
      validatePersistenceRequests(0)
      validatePublishSnsMessageRequests(numberOfRequests = 0)
      validateErrorBucketPutRequests(numberOfErrors = 0)
      validateGetTaggingRequests(metadataObjectKey, 0, recordObjectKey, 0)
      validatePutTaggingRequests(metadataObjectKey, 0, recordObjectKey, 0, defaultPutTagsBodyXml())
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

      validateGetObjectKeysRequests(numberOfRequests = 0, assetSource = assetSource)
      validateMetadataReadRequests(numberOfRequests = 0, metadataObjectKey = metadataObjectKey)
      validateNoObjectTagging(metadataObjectKey, recordObjectKey)
      validateTriggerStepFunctionRequests(numberOfRequests = 0)
      validatePersistenceRequests(0)
      validatePublishSnsMessageRequests(numberOfRequests = 0)
      validateErrorBucketPutRequests(numberOfErrors = 0)
      validateGetTaggingRequests(metadataObjectKey, 0, recordObjectKey, 0)
      validatePutTaggingRequests(metadataObjectKey, 0, recordObjectKey, 0, defaultPutTagsBodyXml())
    }

    s"'handleRequest' with asset source $assetSource" should "throw an error if supplied metadata fails to process" in {
      val invalidSuppliedFields = s""""closure_type": "Open","description": "some kind of description",$invalidSuppliedMetadata"""
      val metadata = metadataJsonString(matchId, defaultFileSize, consignmentId, Some(invalidSuppliedFields), None)
      mockBaseResponses(metadataObjectKey, metadata, assetSource, recordObjectKey)
      mockGraphQlGetConsignmentResponse()

      val mockContext = mock[Context]

      val validMessageBody: String =
        s"""
            {
              "metadataSourceBucket": "$metadataSourceBucket",
              "metadataSourceObjectPrefix": "$userId/$assetSource/$consignmentId/$metadataCategory",
              "recordsSourceBucket": "$recordsSourceBucket",
              "recordsSourceObjectPrefix": "$userId/$assetSource/$consignmentId/$recordsCategory",
              "dataLoadErrors": false,
              "ignoreSiteName": false,
              "loadedNumberOfFiles": 2
            }
            """.stripMargin

      val message = new SQSMessage()
      message.setBody(validMessageBody)
      val messages: java.util.List[SQSMessage] = List(message).asJava
      val sqsEvent = new SQSEvent()
      sqsEvent.setRecords(messages)

      new AggregateProcessingLambda().handleRequest(sqsEvent, mockContext)

      validateGetObjectKeysRequests(assetSource = assetSource)
      validateMetadataReadRequests(metadataObjectKey = metadataObjectKey)
      validateTriggerStepFunctionRequests(numberOfRequests = 0)
      validatePersistenceRequests(3)
      validatePublishSnsMessageRequests()
      validateErrorBucketPutRequests(numberOfErrors = 2)

      val expectedNetworkDriveTagBodyXml = defaultPutTagsBodyXml("CompletedWithIssues")

      if (assetSource == NetworkDrive.id.toLowerCase) {
        validateGetTaggingRequests(metadataObjectKey, 2, recordObjectKey, 0)
        validatePutTaggingRequests(metadataObjectKey, 1, recordObjectKey, 0, expectedNetworkDriveTagBodyXml)
      } else {
        validateGetTaggingRequests(metadataObjectKey, 2, recordObjectKey, 0)
        validatePutTaggingRequests(metadataObjectKey, 1, recordObjectKey, 0, defaultPutTagsBodyXml("CompletedWithIssues"))
      }
    }

    s"'handleRequest' with asset source $assetSource" should
      "upload a draft-metadata.csv to S3 when supplied metadata present in uploaded metadata" in {
        val metadata = metadataJsonString(matchId, defaultFileSize, consignmentId, Some(defaultSuppliedFields), None)
        mockBaseResponses(metadataObjectKey, metadata, assetSource, recordObjectKey)
        mockGraphQlGetConsignmentResponse()

        val mockContext = mock[Context]

        val validMessageBody: String =
          s"""
            {
              "metadataSourceBucket": "$metadataSourceBucket",
              "metadataSourceObjectPrefix": "$userId/$assetSource/$consignmentId/$metadataCategory",
              "recordsSourceBucket": "$recordsSourceBucket",
              "recordsSourceObjectPrefix": "$userId/$assetSource/$consignmentId/$recordsCategory",
              "dataLoadErrors": false,
              "ignoreSiteName": false,
              "loadedNumberOfFiles": 2
            }
            """.stripMargin

        val message = new SQSMessage()
        message.setBody(validMessageBody)
        val messages: java.util.List[SQSMessage] = List(message).asJava
        val sqsEvent = new SQSEvent()
        sqsEvent.setRecords(messages)

        new AggregateProcessingLambda().handleRequest(sqsEvent, mockContext)

        validateGetObjectKeysRequests(assetSource = assetSource)
        validateMetadataReadRequests(metadataObjectKey = metadataObjectKey)
        validateTriggerStepFunctionRequests(numberOfRequests = 2)
        validatePersistenceRequests(8)
        validatePublishSnsMessageRequests()
        validateErrorBucketPutRequests(numberOfErrors = 0)
        validateGetTaggingRequests(metadataObjectKey, 2, recordObjectKey, 0)
        validatePutTaggingRequests(metadataObjectKey, 1, recordObjectKey, 0, defaultPutTagsBodyXml())

        wiremockS3.verify(
          exactly(1),
          putRequestedFor(anyUrl())
            .withUrl(s"/draftMetadataBucket/$consignmentId/draft-metadata.csv")
            .withRequestBody(
              containing(
                "filepath,filename,date last modified,date of the record,description,former reference,closure status,closure start date,closure period,foi exemption code,foi schedule date,is filename closed,alternate filename,is description closed,alternate description,language,translated filename,copyright,copyright details,related material,restrictions on use,evidence provided by,note"
              )
            )
            .withRequestBody(containing(s"$expectedFilePath,file1.txt,2025-07-03,,some kind of description,,Open,,,,,No,,No,,English,,Crown,,,,,"))
        )
      }
  }

  "'handleRequest'" should "handle where no valid assets to process correctly" in {
    val assetSource = SharePoint.id
    val metadataObjectKey = getMetadataObjectKey(assetSource)
    val recordObjectKey = getRecordObjectKey(assetSource)
    val invalidNameMetadata = s"""{
      "Length": "12",
      "Modified": "2025-07-03T09:19:47Z",
      "FileLeafRef": "thumbs.db",
      "FileRef": "/sites/Retail/Shared Documents/aFolder/thumbs.db",
      "sha256ClientSideChecksum": "1b47903dfdf5f21abeb7b304efb8e801656bff31225f522406f45c21a68eddf2",
      "matchId": "$matchId",
      "transferId": "$consignmentId"
    }""".stripMargin

    val expectedPutTagsRequestXml: String =
      s"""<?xml version="1.0" encoding="UTF-8"?><Tagging xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
         |<TagSet><Tag><Key>GuardDutyMalwareScanStatus</Key>
         |<Value>NO_THREATS_FOUND</Value></Tag>
         |<Tag><Key>IGNORE_OBJECT</Key>
         |<Value>TRUE</Value></Tag>
         |</TagSet></Tagging>""".stripMargin.replaceAll("\\n", "")

    mockBaseResponses(metadataObjectKey, invalidNameMetadata, assetSource, recordObjectKey)
    mockS3GetObjectStream(metadataObjectKey, invalidNameMetadata)
    mockGraphQlGetConsignmentResponse()

    val mockContext = mock[Context]

    val message = new SQSMessage()
    message.setBody(validMessageBody(assetSource))
    val messages: java.util.List[SQSMessage] = List(message).asJava
    val sqsEvent = new SQSEvent()
    sqsEvent.setRecords(messages)
    new AggregateProcessingLambda().handleRequest(sqsEvent, mockContext)

    validateGetObjectKeysRequests(assetSource = assetSource)
    validateMetadataReadRequests(metadataObjectKey = metadataObjectKey)
    validateTriggerStepFunctionRequests(numberOfRequests = 0)
    validatePublishSnsMessageRequests()
    validatePersistenceRequests(3)
    validateErrorBucketPutRequests(numberOfErrors = 2)
    validateGetTaggingRequests(metadataObjectKey, 2, recordObjectKey, 1)
    validatePutTaggingRequests(metadataObjectKey, 1, recordObjectKey, 1, expectedPutTagsRequestXml)
  }

  private def mockBaseResponses(metadataObjectKey: String, metadata: String, assetSource: String, recordObjectKey: String): StubMapping = {
    authOkJson()
    mockS3GetObjectTagging(metadataObjectKey)
    mockS3GetObjectTagging(recordObjectKey)
    mockS3GetObjectStream(metadataObjectKey, metadata)
    mockS3PutObjectTagging(metadataObjectKey)
    mockS3PutObjectTagging(recordObjectKey)
    mockS3ListBucketResponse(userId, consignmentId, List(matchId), assetSource)
    mockS3HeadObjectResponse(recordObjectKey)
    mockSfnResponseOk()
    mockGraphQlAddFilesAndMetadataResponse
    mockGraphQlUpdateConsignmentStatusResponse
    mockGraphQlUpdateParentFolderResponse
    mockGraphQlAddConsignmentStatusResponse
    mockS3PutObject()
    mockGetUserDetails(userId)
    mockSnsResponseOk()
    mockGraphQlUpdateDraftMetadataFileNameResponse
    mockS3HeadObjectResponse(recordObjectKey)
  }

  private def validateGetObjectKeysRequests(numberOfRequests: Int = 1, assetSource: String): Unit = {
    wiremockS3.verify(
      exactly(numberOfRequests),
      getRequestedFor(anyUrl())
        .withUrl(s"/?list-type=2&max-keys=1000&prefix=$userId%2F$assetSource%2F$consignmentId%2F$metadataCategory")
    )
  }

  private def validateMetadataReadRequests(numberOfRequests: Int = 1, metadataObjectKey: String): Unit = {
    wiremockS3.verify(
      exactly(numberOfRequests),
      getRequestedFor(anyUrl())
        .withUrl(s"/$metadataObjectKey?partNumber=1")
    )
  }

  private def validateTriggerStepFunctionRequests(numberOfRequests: Int = 1): Unit = {
    wiremockSfnServer.verify(
      exactly(numberOfRequests),
      postRequestedFor(anyUrl())
        .withRequestBody(containing(s"transfer_service_$consignmentId"))
    )
  }

  private def validatePublishSnsMessageRequests(numberOfRequests: Int = 1): Unit = {
    wiremockSnsServer.verify(
      exactly(numberOfRequests),
      postRequestedFor(anyUrl())
    )
  }

  private def validatePersistenceRequests(numberOfCalls: Int): Unit = {
    wiremockGraphqlServer.verify(
      exactly(numberOfCalls),
      postRequestedFor(anyUrl())
        .withUrl("/graphql")
    )
  }

  private def validateErrorBucketPutRequests(numberOfErrors: Int): Unit = {
    wiremockS3.verify(
      exactly(numberOfErrors),
      putRequestedFor(urlMatching(s"/transferErrorBucket/$consignmentId/load/.*"))
    )
  }

  private def validateGetTaggingRequests(metadataObjectKey: String, numberOfMetadataRequests: Int, recordObjectKey: String, numberOfRecordRequests: Int): Unit = {
    wiremockS3.verify(
      exactly(numberOfMetadataRequests),
      getRequestedFor(urlMatching(s"/$metadataObjectKey\\?tagging"))
    )

    wiremockS3.verify(
      exactly(numberOfRecordRequests),
      getRequestedFor(urlMatching(s"/$recordObjectKey\\?tagging"))
    )
  }

  private def validatePutTaggingRequests(
      metadataObjectKey: String,
      numberOfMetadataRequests: Int,
      recordObjectKey: String,
      numberOfRecordRequests: Int,
      expectedBodyXml: String
  ): Unit = {
    wiremockS3.verify(
      exactly(numberOfMetadataRequests),
      putRequestedFor(urlMatching(s"/$metadataObjectKey\\?tagging"))
        .withRequestBody(equalToXml(expectedBodyXml))
    )

    wiremockS3.verify(
      exactly(numberOfRecordRequests),
      putRequestedFor(urlMatching(s"/$recordObjectKey\\?tagging"))
        .withRequestBody(equalToXml(expectedBodyXml))
    )
  }

  private def validateNoObjectTagging(metadataObjectKey: String, recordObjectKey: String): Unit = {
    wiremockS3.verify(
      exactly(0),
      getRequestedFor(anyUrl())
        .withUrl(s"/$metadataObjectKey?tagging")
    )

    wiremockS3.verify(
      exactly(0),
      putRequestedFor(anyUrl())
        .withUrl(s"/$metadataObjectKey?tagging")
    )

    wiremockS3.verify(
      exactly(0),
      getRequestedFor(anyUrl())
        .withUrl(s"/$recordObjectKey?tagging")
    )

    wiremockS3.verify(
      exactly(0),
      putRequestedFor(anyUrl())
        .withUrl(s"/$recordObjectKey?tagging")
    )
  }

  private def defaultPutTagsBodyXml(assetProcessingTagValue: String = "Completed"): String =
    s"""<Tagging xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
          <TagSet>
            <Tag>
              <Key>GuardDutyMalwareScanStatus</Key>
              <Value>NO_THREATS_FOUND</Value>
            </Tag>
            <Tag>
              <Key>ASSET_PROCESSING</Key>
              <Value>$assetProcessingTagValue</Value>
            </Tag>
          </TagSet>
        </Tagging>""".stripMargin.replaceAll("\\n", "")
}
