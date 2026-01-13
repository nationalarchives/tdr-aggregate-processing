package uk.gov.nationalarchives.aggregate.processing.modules

import com.typesafe.scalalogging.Logger
import org.mockito.ArgumentCaptor
import org.mockito.Mockito.{times, verify}
import org.mockito.MockitoSugar.{mock, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.{Logger => UnderlyingLogger}
import uk.gov.nationalarchives.aggregate.processing.AggregateProcessingLambda.AggregateProcessingError
import uk.gov.nationalarchives.aggregate.processing.modules.assetprocessing.AssetProcessing.AssetProcessingError
import uk.gov.nationalarchives.aggregate.processing.modules.orchestration.TransferOrchestration.TransferError
import uk.gov.nationalarchives.aws.utils.s3.S3Utils

class ErrorHandlingSpec extends AnyFlatSpec {
  "handleError" should "log AssetProccessingError messages & upload the error to s3" in {
    val mockLogger = mock[UnderlyingLogger]
    val mockS3Utils = mock[S3Utils]
    val error = AssetProcessingError(
      consignmentId = Some("consignmentId123"),
      matchId = Some("matchId456"),
      source = Some("sourceSystem"),
      errorCode = "ASSET_PROCESSING.TEST_ERROR",
      errorMsg = "Test error message"
    )
    when(mockLogger.isErrorEnabled()).thenReturn(true)

    ErrorHandling.handleError(error, Logger(mockLogger), mockS3Utils)

    verify(mockLogger).error(
      "AssetProcessingError: consignmentId: Some(consignmentId123), matchId: Some(matchId456), source: Some(sourceSystem), errorCode: ASSET_PROCESSING.TEST_ERROR, errorMessage: Test error message"
    )

    val bucketCaptor = ArgumentCaptor.forClass(classOf[String])
    val keyCaptor = ArgumentCaptor.forClass(classOf[String])
    val fileCaptor = ArgumentCaptor.forClass(classOf[java.nio.file.Path])
    verify(mockS3Utils, times(1)).upload(bucketCaptor.capture(), keyCaptor.capture(), fileCaptor.capture())

    assert(bucketCaptor.getValue.contains("tdr-transfer-errors"))
    assert(keyCaptor.getValue.contains("consignmentId123"))
    assert(keyCaptor.getValue.endsWith(".error"))
  }

  it should "log an error when optional fields are None and upload to s3" in {
    val mockLogger = mock[UnderlyingLogger]
    val mockS3Utils = mock[S3Utils]
    val error = AssetProcessingError(
      consignmentId = None,
      matchId = None,
      source = None,
      errorCode = "ASSET_PROCESSING.TEST_ERROR",
      errorMsg = "Test error message"
    )
    when(mockLogger.isErrorEnabled()).thenReturn(true)

    ErrorHandling.handleError(error, Logger(mockLogger), mockS3Utils)
    verify(mockLogger).error(
      "AssetProcessingError: consignmentId: None, matchId: None, source: None, errorCode: ASSET_PROCESSING.TEST_ERROR, errorMessage: Test error message"
    )

    val keyCaptor = ArgumentCaptor.forClass(classOf[String])
    verify(mockS3Utils).upload(org.mockito.ArgumentMatchers.any(), keyCaptor.capture(), org.mockito.ArgumentMatchers.any())
    assert(keyCaptor.getValue.contains("unknown"))
    assert(keyCaptor.getValue.endsWith(".error"))
  }

  it should "log AggregateProcessingError messages & upload the error to s3" in {
    val mockLogger = mock[UnderlyingLogger]
    val mockS3Utils = mock[S3Utils]
    val consignmentId = java.util.UUID.randomUUID()
    val error = AggregateProcessingError(
      consignmentId = consignmentId,
      errorCode = "AGGREGATE_PROCESSING.TEST_ERROR",
      errorMessage = "Test error message"
    )
    when(mockLogger.isErrorEnabled()).thenReturn(true)

    ErrorHandling.handleError(error, Logger(mockLogger), mockS3Utils)
    verify(mockLogger).error(
      s"AggregateProcessingError: consignmentId: $consignmentId, errorCode: AGGREGATE_PROCESSING.TEST_ERROR, errorMessage: Test error message"
    )

    val bucketCaptor = ArgumentCaptor.forClass(classOf[String])
    val keyCaptor = ArgumentCaptor.forClass(classOf[String])
    val fileCaptor = ArgumentCaptor.forClass(classOf[java.nio.file.Path])
    verify(mockS3Utils, times(1)).upload(bucketCaptor.capture(), keyCaptor.capture(), fileCaptor.capture())
    assert(keyCaptor.getValue.contains(consignmentId.toString))
    assert(keyCaptor.getValue.endsWith(".error"))
  }

  it should "log TransferError messages & upload the error to s3" in {
    val mockLogger = mock[UnderlyingLogger]
    val mockS3Utils = mock[S3Utils]
    val consignmentId = java.util.UUID.randomUUID()
    val error = TransferError(
      consignmentId = Some(consignmentId),
      errorCode = "AGGREGATE_PROCESSING.TEST_ERROR",
      errorMessage = "Test error message"
    )
    when(mockLogger.isErrorEnabled()).thenReturn(true)

    ErrorHandling.handleError(error, Logger(mockLogger), mockS3Utils)
    verify(mockLogger).error(
      s"TransferError: consignmentId: Some($consignmentId), errorCode: AGGREGATE_PROCESSING.TEST_ERROR, errorMessage: Test error message"
    )

    val bucketCaptor = ArgumentCaptor.forClass(classOf[String])
    val keyCaptor = ArgumentCaptor.forClass(classOf[String])
    val fileCaptor = ArgumentCaptor.forClass(classOf[java.nio.file.Path])
    verify(mockS3Utils, times(1)).upload(bucketCaptor.capture(), keyCaptor.capture(), fileCaptor.capture())
    assert(keyCaptor.getValue.contains("unassociated/TransferError"))
    assert(keyCaptor.getValue.endsWith(".error"))
  }
}
