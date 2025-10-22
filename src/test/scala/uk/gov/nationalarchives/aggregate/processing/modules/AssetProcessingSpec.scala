package uk.gov.nationalarchives.aggregate.processing.modules

import com.typesafe.scalalogging.Logger
import graphql.codegen.types.ClientSideMetadataInput
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar.{mock, times, verify, when}
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.slf4j.{Logger => UnderlyingLogger}
import software.amazon.awssdk.core.ResponseBytes
import software.amazon.awssdk.core.async.AsyncResponseTransformer
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.{GetObjectRequest, GetObjectResponse, GetObjectTaggingRequest, GetObjectTaggingResponse}
import software.amazon.awssdk.utils.CompletableFutureUtils.failedFuture
import uk.gov.nationalarchives.aggregate.processing.ExternalServiceSpec
import uk.gov.nationalarchives.aggregate.processing.modules.AssetProcessing.{AssetProcessingResult, SuppliedMetadata, SystemMetadata}
import uk.gov.nationalarchives.aws.utils.s3.S3Utils

import java.io.ByteArrayInputStream
import java.util.UUID
import java.util.concurrent.CompletableFuture

class AssetProcessingSpec extends ExternalServiceSpec {
  private val userId = UUID.randomUUID()
  private val consignmentId = UUID.randomUUID()
  private val matchId = UUID.randomUUID().toString
  private val defaultMetadataJsonString = s"""{
      "Length": "12",
      "Modified": "2025-07-03T09:19:47Z",
      "FileLeafRef": "file1.txt",
      "FileRef": "/sites/Retail/Shared Documents/file1.txt",
      "SHA256ClientSideChecksum": "1b47903dfdf5f21abeb7b304efb8e801656bff31225f522406f45c21a68eddf2",
      "matchId": "$matchId",
      "transferId": "$consignmentId"
    }""".stripMargin

  "processAsset" should "return asset processing result and not log errors when metadata json is valid" in {
    val mockLogger = mock[UnderlyingLogger]
    val s3UtilsMock = mock[S3Utils]

    val expectedInput = ClientSideMetadataInput(
      "sites/Retail/Shared Documents/file1.txt",
      "1b47903dfdf5f21abeb7b304efb8e801656bff31225f522406f45c21a68eddf2",
      1751534387000L,
      12L,
      matchId
    )

    val expectedResult = AssetProcessingResult(
      Some(matchId),
      processingErrors = false,
      Some(expectedInput),
      systemMetadata = List(
        SystemMetadata("FileRef", "/sites/Retail/Shared Documents/file1.txt"),
        SystemMetadata("FileLeafRef", "file1.txt"),
        SystemMetadata("Modified", "2025-07-03T09:19:47Z"),
        SystemMetadata("Length", "12")
      )
    )

    when(mockLogger.isInfoEnabled()).thenReturn(true)
    when(mockLogger.isErrorEnabled).thenReturn(true)
    when(s3UtilsMock.getObjectAsStream(any[String], any[String]))
      .thenReturn(new ByteArrayInputStream(defaultMetadataJsonString.getBytes("UTF-8")))

    val assetProcessing = new AssetProcessing(s3UtilsMock)(Logger(mockLogger))
    val result = assetProcessing.processAsset("s3Bucket", s"$userId/sharepoint/$consignmentId/metadata/$matchId.metadata")

    result shouldEqual expectedResult

    verify(s3UtilsMock, times(1)).addObjectTags("s3Bucket", s"$userId/sharepoint/$consignmentId/metadata/$matchId.metadata", Map("ASSET_PROCESSING" -> "Completed"))

    verify(mockLogger, times(0)).isErrorEnabled
    verifyDefaultInfoLogging(mockLogger)
    verify(mockLogger).info("Asset metadata successfully processed for: {}", s"$userId/sharepoint/$consignmentId/metadata/$matchId.metadata")
  }

  "processAsset" should "log an error and return asset processing result when the object key is invalid" in {
    val mockLogger = mock[UnderlyingLogger]
    val s3UtilsMock = mock[S3Utils]
    val expectedResult = AssetProcessingResult(None, processingErrors = true, None)

    when(mockLogger.isInfoEnabled()).thenReturn(true)
    when(mockLogger.isErrorEnabled()).thenReturn(true)

    val assetProcessing = new AssetProcessing(s3UtilsMock)(Logger(mockLogger))

    val result = assetProcessing.processAsset("s3Bucket", "incorrect/object/key/format.txt")

    result shouldEqual expectedResult

    verify(s3UtilsMock, times(1)).addObjectTags("s3Bucket", "incorrect/object/key/format.txt", Map("ASSET_PROCESSING" -> "CompletedWithIssues"))

    verify(mockLogger).info("Processing asset metadata for: {}", s"incorrect/object/key/format.txt")
    verify(mockLogger).error(
      s"AssetProcessingError: consignmentId: None, matchId: None, source: None, errorCode: ASSET_PROCESSING.OBJECT_KEY.INVALID, errorMessage: Invalid object key: incorrect/object/key/format.txt: Invalid UUID string: incorrect"
    )
  }

  "processAsset" should "log an error and return asset processing result when a required json field is missing" in {
    val mockLogger = mock[UnderlyingLogger]
    val s3UtilsMock = mock[S3Utils]
    val missingFieldJson = s"""{
      "Modified": "2025-07-03T09:19:47Z",
      "FileLeafRef": "file1.txt",
      "FileRef": "/sites/Retail/Shared Documents/file1.txt",
      "SHA256ClientSideChecksum": "1b47903dfdf5f21abeb7b304efb8e801656bff31225f522406f45c21a68eddf2",
      "matchId": "$matchId",
      "transferId": "$consignmentId"
    }""".stripMargin
    val expectedResult = AssetProcessingResult(Some(matchId), processingErrors = true, None)

    when(mockLogger.isErrorEnabled()).thenReturn(true)
    when(mockLogger.isInfoEnabled()).thenReturn(true)
    when(s3UtilsMock.getObjectAsStream(any[String], any[String]))
      .thenReturn(new ByteArrayInputStream(missingFieldJson.getBytes("UTF-8")))

    val assetProcessing = new AssetProcessing(s3UtilsMock)(Logger(mockLogger))
    val result = assetProcessing.processAsset("s3Bucket", s"$userId/sharepoint/$consignmentId/metadata/$matchId.metadata")

    result shouldEqual expectedResult

    verify(s3UtilsMock, times(1)).addObjectTags("s3Bucket", s"$userId/sharepoint/$consignmentId/metadata/$matchId.metadata", Map("ASSET_PROCESSING" -> "CompletedWithIssues"))

    verifyDefaultInfoLogging(mockLogger)
    verify(mockLogger).error(
      s"AssetProcessingError: consignmentId: Some($consignmentId), matchId: Some($matchId), source: Some(sharepoint), errorCode: ASSET_PROCESSING.JSON.INVALID, errorMessage: DecodingFailure at .Length: Missing required field"
    )
  }

  "processAsset" should "log an error and return asset processing result when reading S3 object stream fails" in {
    val mockLogger = mock[UnderlyingLogger]
    val s3AsyncClient = mock[S3AsyncClient]
    val s3UtilsMock = S3Utils(s3AsyncClient)
    val objectTaggingResponse = GetObjectTaggingResponse.builder().build()

    when(mockLogger.isErrorEnabled()).thenReturn(true)
    when(mockLogger.isInfoEnabled()).thenReturn(true)
    when(s3AsyncClient.getObject(any[GetObjectRequest], any[AsyncResponseTransformer[GetObjectResponse, ResponseBytes[GetObjectResponse]]]))
      .thenReturn(failedFuture(new RuntimeException("read failed")))
    when(s3AsyncClient.getObjectTagging(any[GetObjectTaggingRequest]))
      .thenReturn(CompletableFuture.completedFuture(objectTaggingResponse))

    val expectedResult = AssetProcessingResult(Some(matchId), processingErrors = true, None)

    val assetProcessing = new AssetProcessing(s3UtilsMock)(Logger(mockLogger))
    val result = assetProcessing.processAsset("s3Bucket", s"$userId/sharepoint/$consignmentId/metadata/$matchId.metadata")

    result shouldEqual expectedResult

    verify(s3AsyncClient, times(1)).getObjectTagging(
      GetObjectTaggingRequest
        .builder()
        .bucket("s3Bucket")
        .key(s"$userId/sharepoint/$consignmentId/metadata/$matchId.metadata")
        .build()
    )

    verifyDefaultInfoLogging(mockLogger)
    verify(mockLogger).error(
      s"AssetProcessingError: consignmentId: Some($consignmentId), matchId: Some($matchId), source: Some(sharepoint), errorCode: ASSET_PROCESSING.S3.READ_ERROR, errorMessage: java.lang.RuntimeException: read failed"
    )
  }

  "processAsset" should "log an error and return asset processing result when S3 object is non-UTF-8 encoded" in {
    val mockLogger = mock[UnderlyingLogger]
    val s3UtilsMock = mock[S3Utils]
    val invalidUtf8 = Array[Byte](0, -1, -2, -3)
    val expectedResult = AssetProcessingResult(Some(matchId), processingErrors = true, None)

    when(mockLogger.isErrorEnabled()).thenReturn(true)
    when(mockLogger.isInfoEnabled()).thenReturn(true)
    when(s3UtilsMock.getObjectAsStream(any[String], any[String]))
      .thenReturn(new ByteArrayInputStream(invalidUtf8))

    val assetProcessing = new AssetProcessing(s3UtilsMock)(Logger(mockLogger))
    val result = assetProcessing.processAsset("s3Bucket", s"$userId/sharepoint/$consignmentId/metadata/$matchId.metadata")

    result shouldEqual expectedResult

    verify(s3UtilsMock, times(1)).addObjectTags("s3Bucket", s"$userId/sharepoint/$consignmentId/metadata/$matchId.metadata", Map("ASSET_PROCESSING" -> "CompletedWithIssues"))

    verifyDefaultInfoLogging(mockLogger)
    verify(mockLogger).error(
      s"AssetProcessingError: consignmentId: Some($consignmentId), matchId: Some($matchId), source: Some(sharepoint), errorCode: ASSET_PROCESSING.ENCODING.INVALID, errorMessage: Invalid UTF-8 Sequence, expecting: 4bytes, but got: 3bytes - reached end of stream. @ byte position: -1"
    )
  }

  "processAsset" should "log an error and return asset processing result when S3 object is not valid json" in {
    val mockLogger = mock[UnderlyingLogger]
    val s3UtilsMock = mock[S3Utils]
    val nonJsonString = "some random string".getBytes("UTF-8")
    val expectedResult = AssetProcessingResult(Some(matchId), processingErrors = true, None)

    when(mockLogger.isErrorEnabled()).thenReturn(true)
    when(mockLogger.isInfoEnabled()).thenReturn(true)
    when(s3UtilsMock.getObjectAsStream(any[String], any[String]))
      .thenReturn(new ByteArrayInputStream(nonJsonString))

    val assetProcessing = new AssetProcessing(s3UtilsMock)(Logger(mockLogger))
    val result = assetProcessing.processAsset("s3Bucket", s"$userId/sharepoint/$consignmentId/metadata/$matchId.metadata")

    result shouldEqual expectedResult

    verify(s3UtilsMock, times(1)).addObjectTags("s3Bucket", s"$userId/sharepoint/$consignmentId/metadata/$matchId.metadata", Map("ASSET_PROCESSING" -> "CompletedWithIssues"))

    verifyDefaultInfoLogging(mockLogger)
    verify(mockLogger).error(
      s"AssetProcessingError: consignmentId: Some($consignmentId), matchId: Some($matchId), source: Some(sharepoint), errorCode: ASSET_PROCESSING.JSON.INVALID, errorMessage: expected json value got 'some r...' (line 1, column 1)"
    )
  }

  "processAsset" should "log an error and return asset processing result when json is an unknown structure" in {
    val mockLogger = mock[UnderlyingLogger]
    val s3UtilsMock = mock[S3Utils]
    val unknownJsonString = s"""{"aField": "aValue", "bField": "bValue}"""
    val expectedResult = AssetProcessingResult(Some(matchId), processingErrors = true, None)

    when(mockLogger.isErrorEnabled()).thenReturn(true)
    when(mockLogger.isInfoEnabled()).thenReturn(true)
    when(s3UtilsMock.getObjectAsStream(any[String], any[String]))
      .thenReturn(new ByteArrayInputStream(unknownJsonString.getBytes("UTF-8")))

    val assetProcessing = new AssetProcessing(s3UtilsMock)(Logger(mockLogger))
    val result = assetProcessing.processAsset("s3Bucket", s"$userId/sharepoint/$consignmentId/metadata/$matchId.metadata")

    result shouldEqual expectedResult

    verify(s3UtilsMock, times(1)).addObjectTags("s3Bucket", s"$userId/sharepoint/$consignmentId/metadata/$matchId.metadata", Map("ASSET_PROCESSING" -> "CompletedWithIssues"))

    verifyDefaultInfoLogging(mockLogger)
    verify(mockLogger).error(
      s"AssetProcessingError: consignmentId: Some($consignmentId), matchId: Some($matchId), source: Some(sharepoint), errorCode: ASSET_PROCESSING.JSON.INVALID, errorMessage: exhausted input"
    )
  }

  "processAsset" should "log an error and return asset processing result when the event matchId does not match the metadata matchId" in {
    val mockLogger = mock[UnderlyingLogger]
    val s3UtilsMock = mock[S3Utils]
    val differentMatchId = UUID.randomUUID()
    val expectedResult = AssetProcessingResult(None, processingErrors = true, None)

    when(mockLogger.isErrorEnabled()).thenReturn(true)
    when(mockLogger.isInfoEnabled()).thenReturn(true)
    when(s3UtilsMock.getObjectAsStream(any[String], any[String]))
      .thenReturn(new ByteArrayInputStream(defaultMetadataJsonString.getBytes("UTF-8")))

    val assetProcessing = new AssetProcessing(s3UtilsMock)(Logger(mockLogger))
    val result = assetProcessing.processAsset("s3Bucket", s"$userId/sharepoint/$consignmentId/metadata/$differentMatchId.metadata")

    result shouldEqual expectedResult

    verify(s3UtilsMock, times(1)).addObjectTags(
      "s3Bucket",
      s"$userId/sharepoint/$consignmentId/metadata/$differentMatchId.metadata",
      Map("ASSET_PROCESSING" -> "CompletedWithIssues")
    )

    verify(mockLogger).error(
      s"AssetProcessingError: consignmentId: Some($consignmentId), matchId: None, source: Some(sharepoint), errorCode: ASSET_PROCESSING.MATCH_ID.MISMATCH, errorMessage: Mismatched match ids: $differentMatchId and $matchId"
    )
  }

  "processAsset" should "return asset processing result and not log errors when supplied metadata json is valid" in {
    val mockLogger = mock[UnderlyingLogger]
    val s3UtilsMock = mock[S3Utils]

    val defaultMetadataWithSupplied = s"""{
      "Length": "12",
      "Modified": "2025-07-03T09:19:47Z",
      "FileLeafRef": "file1.txt",
      "FileRef": "/sites/Retail/Shared Documents/file1.txt",
      "SHA256ClientSideChecksum": "1b47903dfdf5f21abeb7b304efb8e801656bff31225f522406f45c21a68eddf2",
      "matchId": "$matchId",
      "transferId": "$consignmentId",
      "filepath": "file/Path/1",
      "description": "some kind of description"
    }""".stripMargin

    val jsonMetadataString = defaultMetadataWithSupplied

    val expectedInput = ClientSideMetadataInput(
      "sites/Retail/Shared Documents/file1.txt",
      "1b47903dfdf5f21abeb7b304efb8e801656bff31225f522406f45c21a68eddf2",
      1751534387000L,
      12L,
      matchId
    )

    val expectedResult =
      AssetProcessingResult(
        Some(matchId),
        processingErrors = false,
        Some(expectedInput),
        systemMetadata = List(
          SystemMetadata("FileRef", "/sites/Retail/Shared Documents/file1.txt"),
          SystemMetadata("FileLeafRef", "file1.txt"),
          SystemMetadata("Modified", "2025-07-03T09:19:47Z"),
          SystemMetadata("Length", "12")
        ),
        suppliedMetadata = List(SuppliedMetadata("description", "some kind of description"))
      )

    when(mockLogger.isInfoEnabled()).thenReturn(true)
    when(mockLogger.isErrorEnabled).thenReturn(true)
    when(s3UtilsMock.getObjectAsStream(any[String], any[String]))
      .thenReturn(new ByteArrayInputStream(jsonMetadataString.getBytes("UTF-8")))

    val assetProcessing = new AssetProcessing(s3UtilsMock)(Logger(mockLogger))
    val result = assetProcessing.processAsset("s3Bucket", s"$userId/sharepoint/$consignmentId/metadata/$matchId.metadata")

    result shouldEqual expectedResult

    verify(mockLogger, times(0)).isErrorEnabled
    verifyDefaultInfoLogging(mockLogger)
    verify(mockLogger).info("Asset metadata successfully processed for: {}", s"$userId/sharepoint/$consignmentId/metadata/$matchId.metadata")
  }

  private def verifyDefaultInfoLogging(logger: UnderlyingLogger): Unit = {
    verify(logger).info("Processing asset metadata for: {}", s"$userId/sharepoint/$consignmentId/metadata/$matchId.metadata")
  }
}
