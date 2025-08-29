package uk.gov.nationalarchives.aggregate.processing.modules

import com.typesafe.scalalogging.Logger
import graphql.codegen.types.ClientSideMetadataInput
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar.{mock, verify, when}
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.slf4j.{Logger => UnderlyingLogger}
import software.amazon.awssdk.core.ResponseBytes
import software.amazon.awssdk.core.async.AsyncResponseTransformer
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.{GetObjectRequest, GetObjectResponse}
import software.amazon.awssdk.utils.CompletableFutureUtils.failedFuture
import uk.gov.nationalarchives.aggregate.processing.ExternalServiceSpec
import uk.gov.nationalarchives.aggregate.processing.modules.AssetProcessing.AssetProcessingResult
import uk.gov.nationalarchives.aws.utils.s3.S3Utils

import java.io.ByteArrayInputStream
import java.util.UUID

class AssetProcessingSpec extends ExternalServiceSpec {
  private val consignmentId = UUID.randomUUID()
  private val matchId = UUID.randomUUID().toString
  private val defaultMetadataJsonString = s"""{
      "Length": "12",
      "Modified": "2025-07-03T09:19:47Z",
      "FileLeafRef": "file1.txt",
      "FileRef": "/sites/Retail/Shared Documents/file1.txt",
      "sha256ClientSideChecksum": "1b47903dfdf5f21abeb7b304efb8e801656bff31225f522406f45c21a68eddf2",
      "matchId": "$matchId",
      "transferId": "$consignmentId"
    }""".stripMargin

  "processAsset" should "not log errors and return correct result when metadata json is valid" in {
    val mockLogger = mock[UnderlyingLogger]
    val s3UtilsMock = mock[S3Utils]

    val expectedInput = ClientSideMetadataInput(
      "/sites/Retail/Shared Documents/file1.txt",
      "1b47903dfdf5f21abeb7b304efb8e801656bff31225f522406f45c21a68eddf2",
      1751534387000L,
      12L,
      matchId
    )

    val expectedResult = AssetProcessingResult(Some(matchId), processingErrors = false, Some(expectedInput))

    when(s3UtilsMock.getObjectAsStream(any[String], any[String]))
      .thenReturn(new ByteArrayInputStream(defaultMetadataJsonString.getBytes("UTF-8")))

    val assetProcessing = new AssetProcessing(s3UtilsMock)(Logger(mockLogger))
    val result = assetProcessing.processAsset("s3Bucket", s"user/sharepoint/$consignmentId/metadata/$matchId.metadata")

    result shouldEqual expectedResult
  }

  "processAsset" should "log an error and return the correct result when the object key is in an invalid format" in {
    val mockLogger = mock[UnderlyingLogger]
    val s3UtilsMock = mock[S3Utils]
    val expectedResult = AssetProcessingResult(None, processingErrors = true, None)

    when(mockLogger.isErrorEnabled()).thenReturn(true)

    val assetProcessing = new AssetProcessing(s3UtilsMock)(Logger(mockLogger))

    val result = assetProcessing.processAsset("s3Bucket", "incorrect/object/key/format.txt")

    result shouldEqual expectedResult

    verify(mockLogger).error(
      s"AssetProcessingError: consignmentId: None, matchId: None, source: None, errorCode: ASSET_PROCESSING.OBJECT_KEY.INVALID, errorMessage: Invalid object key: incorrect/object/key/format.txt"
    )
  }

  "processAsset" should "log an error and return correct result when required json field missing" in {
    val mockLogger = mock[UnderlyingLogger]
    val s3UtilsMock = mock[S3Utils]
    val missingFieldJson = s"""{
      "Modified": "2025-07-03T09:19:47Z",
      "FileLeafRef": "file1.txt",
      "FileRef": "/sites/Retail/Shared Documents/file1.txt",
      "sha256ClientSideChecksum": "1b47903dfdf5f21abeb7b304efb8e801656bff31225f522406f45c21a68eddf2",
      "matchId": "$matchId",
      "transferId": "$consignmentId"
    }""".stripMargin
    val expectedResult = AssetProcessingResult(Some(matchId), processingErrors = true, None)

    when(mockLogger.isErrorEnabled()).thenReturn(true)
    when(s3UtilsMock.getObjectAsStream(any[String], any[String]))
      .thenReturn(new ByteArrayInputStream(missingFieldJson.getBytes("UTF-8")))

    val assetProcessing = new AssetProcessing(s3UtilsMock)(Logger(mockLogger))
    val result = assetProcessing.processAsset("s3Bucket", s"user/sharepoint/$consignmentId/metadata/$matchId.metadata")

    result shouldEqual expectedResult

    verify(mockLogger).error(
      s"AssetProcessingError: consignmentId: Some($consignmentId), matchId: Some($matchId), source: Some(sharepoint), errorCode: ASSET_PROCESSING.MISSING_FIELD.Length, errorMessage: "
    )
  }

  "processAsset" should "log an error message when reading the object from s3 as a stream fails" in {
    val mockLogger = mock[UnderlyingLogger]
    val s3AsyncClient = mock[S3AsyncClient]
    val s3UtilsMock = S3Utils(s3AsyncClient)
    when(mockLogger.isErrorEnabled()).thenReturn(true)
    when(s3AsyncClient.getObject(any[GetObjectRequest], any[AsyncResponseTransformer[GetObjectResponse, ResponseBytes[GetObjectResponse]]]))
      .thenReturn(failedFuture(new RuntimeException("read failed")))

    val expectedResult = AssetProcessingResult(Some(matchId), processingErrors = true, None)

    val assetProcessing = new AssetProcessing(s3UtilsMock)(Logger(mockLogger))
    val result = assetProcessing.processAsset("s3Bucket", s"user/sharepoint/$consignmentId/metadata/$matchId.metadata")

    result shouldEqual expectedResult

    verify(mockLogger).error(
      s"AssetProcessingError: consignmentId: Some($consignmentId), matchId: Some($matchId), source: Some(sharepoint), errorCode: ASSET_PROCESSING.S3.READ_ERROR, errorMessage: java.lang.RuntimeException: read failed"
    )
  }

  "processAsset" should "log an error and return correct result when S3 Json object is non-UTF-8" in {
    val mockLogger = mock[UnderlyingLogger]
    val s3UtilsMock = mock[S3Utils]
    val invalidUtf8 = Array[Byte](0, -1, -2, -3)
    val expectedResult = AssetProcessingResult(Some(matchId), processingErrors = true, None)
    when(s3UtilsMock.getObjectAsStream(any[String], any[String]))
      .thenReturn(new ByteArrayInputStream(invalidUtf8))
    when(mockLogger.isErrorEnabled()).thenReturn(true)

    val assetProcessing = new AssetProcessing(s3UtilsMock)(Logger(mockLogger))
    val result = assetProcessing.processAsset("s3Bucket", s"user/sharepoint/$consignmentId/metadata/$matchId.metadata")

    result shouldEqual expectedResult

    verify(mockLogger).error(
      s"AssetProcessingError: consignmentId: Some($consignmentId), matchId: Some($matchId), source: Some(sharepoint), errorCode: ASSET_PROCESSING.UTF.INVALID, errorMessage: Invalid UTF-8 Sequence, expecting: 4bytes, but got: 3bytes - reached end of stream. @ byte position: -1"
    )
  }

  "processAsset" should "log an error and return the correct result when S3 object is not valid json" in {
    val mockLogger = mock[UnderlyingLogger]
    val s3UtilsMock = mock[S3Utils]
    val nonJsonString = "some random string".getBytes("UTF-8")
    val expectedResult = AssetProcessingResult(Some(matchId), processingErrors = true, None)
    when(s3UtilsMock.getObjectAsStream(any[String], any[String]))
      .thenReturn(new ByteArrayInputStream(nonJsonString))
    when(mockLogger.isErrorEnabled()).thenReturn(true)

    val assetProcessing = new AssetProcessing(s3UtilsMock)(Logger(mockLogger))
    val result = assetProcessing.processAsset("s3Bucket", s"user/sharepoint/$consignmentId/metadata/$matchId.metadata")

    result shouldEqual expectedResult

    verify(mockLogger).error(
      s"AssetProcessingError: consignmentId: Some($consignmentId), matchId: Some($matchId), source: Some(sharepoint), errorCode: ASSET_PROCESSING.JSON.INVALID, errorMessage: expected json value got 'some r...' (line 1, column 1)"
    )
  }
}
