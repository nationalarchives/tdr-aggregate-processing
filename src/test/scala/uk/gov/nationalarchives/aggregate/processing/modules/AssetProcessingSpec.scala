package uk.gov.nationalarchives.aggregate.processing.modules

import com.typesafe.scalalogging.Logger
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar.{mock, verify, when}
import org.slf4j.{Logger => UnderlyingLogger}
import software.amazon.awssdk.core.ResponseBytes
import software.amazon.awssdk.core.async.AsyncResponseTransformer
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.{GetObjectRequest, GetObjectResponse}
import software.amazon.awssdk.utils.CompletableFutureUtils.failedFuture
import uk.gov.nationalarchives.aggregate.processing.ExternalServiceSpec
import uk.gov.nationalarchives.aws.utils.s3.S3Utils

import java.io.ByteArrayInputStream

class AssetProcessingSpec extends ExternalServiceSpec {

  "getObjectAsStream" should "return an error message when reading the object from s3 as a stream fails" in {
    val mockLogger = mock[UnderlyingLogger]
    val s3AsyncClient = mock[S3AsyncClient]
    val s3UtilsMock = S3Utils(s3AsyncClient)
    when(mockLogger.isErrorEnabled()).thenReturn(true)
    when(s3AsyncClient.getObject(any[GetObjectRequest], any[AsyncResponseTransformer[GetObjectResponse, ResponseBytes[GetObjectResponse]]]))
      .thenReturn(failedFuture(new RuntimeException("read failed")))

    val assetProcessing = new AssetProcessing(s3UtilsMock)(Logger(mockLogger))
    assetProcessing.processAsset("s3Bucket", "user/sharepoint/consignmentId/metadata/matchId.metadata")

    verify(mockLogger).error(
      "AssetProcessingError: consignmentId: consignmentId, matchId: Some(matchId), source: sharepoint, errorCode: ASSET_PROCESSING.S3.READ_ERROR, errorMessage: java.lang.RuntimeException: read failed"
    )
  }

  "processAsset" should "pass when S3 Json object is valid UTF-8" in {
    val mockLogger = mock[UnderlyingLogger]
    val s3UtilsMock = mock[S3Utils]
    val validUtf8 = """{"key": "value"}""".getBytes("UTF-8")
    when(s3UtilsMock.getObjectAsStream(any[String], any[String]))
      .thenReturn(new ByteArrayInputStream(validUtf8))
    when(mockLogger.isInfoEnabled()).thenReturn(true)

    val assetProcessing = new AssetProcessing(s3UtilsMock)(Logger(mockLogger))
    assetProcessing.processAsset("s3Bucket", "user/sharepoint/consignmentId/metadata/matchId.metadata")

    verify(mockLogger).info(
      "UTF8 validation passed: {}",
      "user/sharepoint/consignmentId/metadata/matchId.metadata"
    )
  }

  "processAsset" should "return error logging if S3 Json object is non-UTF-8" in {
    val mockLogger = mock[UnderlyingLogger]
    val s3UtilsMock = mock[S3Utils]
    val invalidUtf8 = Array[Byte](0, -1, -2, -3)
    when(s3UtilsMock.getObjectAsStream(any[String], any[String]))
      .thenReturn(new ByteArrayInputStream(invalidUtf8))
    when(mockLogger.isErrorEnabled()).thenReturn(true)

    val assetProcessing = new AssetProcessing(s3UtilsMock)(Logger(mockLogger))
    assetProcessing.processAsset("s3Bucket", "user/sharepoint/consignmentId/metadata/matchId.metadata")
    verify(mockLogger).error(
      "AssetProcessingError: consignmentId: consignmentId, matchId: Some(matchId), source: sharepoint, errorCode: ASSET_PROCESSING.UTF.INVALID, errorMessage: Invalid UTF-8 Sequence, expecting: 4bytes, but got: 3bytes - reached end of stream. @ byte position: -1"
    )
  }
}
