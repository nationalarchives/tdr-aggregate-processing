package uk.gov.nationalarchives.aggregate.processing.modules

import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar.{mock, verify, when}
import org.scalatest.matchers.must.Matchers.{noException, thrownBy}
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import uk.gov.nationalarchives.aggregate.processing.ExternalServiceSpec
import uk.gov.nationalarchives.aws.utils.s3.S3Utils
import com.typesafe.scalalogging.Logger
import org.slf4j.{Logger => UnderlyingLogger}

import java.io.ByteArrayInputStream

class AssetProcessingSpec extends ExternalServiceSpec {

  "processAsset" should "pass when S3 Json object is valid UTF-8" in {
    val mockLogger = mock[UnderlyingLogger]
    val s3UtilsMock = mock[S3Utils]
    val validUtf8 = """{"key": "value"}""".getBytes("UTF-8")
    when(s3UtilsMock.getObjectAsStream(any[String], any[String]))
      .thenReturn(new ByteArrayInputStream(validUtf8))

    val assetProcessing = new AssetProcessing(s3UtilsMock)(Logger(mockLogger))

    noException shouldBe thrownBy {
      assetProcessing.processAsset("s3Bucket", "user/sharepoint/consignmentId/metadata/matchId.metadata")
    }
  }

  "processAsset" should "return an exception with error logging if S3 Json object is non-UTF-8" in {
    val mockLogger = mock[UnderlyingLogger]
    val s3UtilsMock = mock[S3Utils]
    val invalidUtf8 = Array[Byte](0, -1, -2, -3)
    when(s3UtilsMock.getObjectAsStream(any[String], any[String]))
      .thenReturn(new ByteArrayInputStream(invalidUtf8))
    when(mockLogger.isErrorEnabled()).thenReturn(true)

    val assetProcessing = new AssetProcessing(s3UtilsMock)(Logger(mockLogger))

    val exception = intercept[RuntimeException] {
      assetProcessing.processAsset("s3Bucket", "user/sharepoint/consignmentId/metadata/matchId.metadata")
    }
    verify(mockLogger).error(
      "AssetProcessingError$: consignmentId: consignmentId, matchId: Some(matchId.metadata), source: sharepoint, errorCode: ASSET_PROCESSING.UTF.INVALID, errorMessage: Invalid UTF-8 Sequence, expecting: 4bytes, but got: 3bytes - reached end of stream. @ byte position: -1")
    exception.getMessage shouldEqual s"UTF8 validation failed for S3 object: s3://s3Bucket/user/sharepoint/consignmentId/metadata/matchId.metadata"
  }
}
