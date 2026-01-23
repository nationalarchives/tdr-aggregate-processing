package uk.gov.nationalarchives.aggregate.processing.modules

import com.typesafe.scalalogging.Logger
import io.circe.syntax.EncoderOps
import org.mockito.ArgumentCaptor
import org.mockito.Mockito.{times, verify}
import org.mockito.MockitoSugar.{mock, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.{Logger => UnderlyingLogger}
import uk.gov.nationalarchives.aggregate.processing.AggregateProcessingLambda.AggregateProcessingError
import uk.gov.nationalarchives.aggregate.processing.modules.assetprocessing.AssetProcessing.AssetProcessingError
import uk.gov.nationalarchives.aggregate.processing.modules.orchestration.TransferOrchestration.TransferError
import uk.gov.nationalarchives.aws.utils.s3.S3Utils

import java.nio.charset.StandardCharsets

class ErrorHandlingSpec extends AnyFlatSpec {
  "handleError" should "log AssetProcessingError messages and upload the error to s3" in {
    val mockLogger = mock[UnderlyingLogger]
    val mockS3Utils = mock[S3Utils]
    val consignmentId = java.util.UUID.randomUUID()
    val errorHandling = new ErrorHandling(mockS3Utils)
    val error = AssetProcessingError(
      consignmentId = Some(consignmentId),
      matchId = Some("matchId456"),
      source = Some("sourceSystem"),
      errorCode = "ASSET_PROCESSING.TEST_ERROR",
      errorMsg = "Test error message"
    )

    val expectedErrorBytes = error.asJson.spaces2.getBytes(StandardCharsets.UTF_8)

    when(mockLogger.isErrorEnabled()).thenReturn(true)

    errorHandling.handleError(error, Logger(mockLogger))

    verify(mockLogger).error(
      s"AssetProcessingError: consignmentId: Some($consignmentId), matchId: Some(matchId456), source: Some(sourceSystem), errorCode: ASSET_PROCESSING.TEST_ERROR, errorMessage: Test error message"
    )

    val bucketCaptor = ArgumentCaptor.forClass(classOf[String])
    val bytesCaptor = ArgumentCaptor.forClass(classOf[Array[Byte]])
    val keyCaptor = ArgumentCaptor.forClass(classOf[String])
    verify(mockS3Utils, times(1)).putObject(bucketCaptor.capture(), bytesCaptor.capture(), keyCaptor.capture())

    assert(bucketCaptor.getValue.contains("transferErrorBucket"))
    assert(bytesCaptor.getValue sameElements expectedErrorBytes)
    assert(keyCaptor.getValue.contains(s"$consignmentId/AssetProcessingError"))
    assert(keyCaptor.getValue.endsWith(".error"))
  }

  it should "log an error when optional fields are None and upload error to s3" in {
    val mockLogger = mock[UnderlyingLogger]
    val mockS3Utils = mock[S3Utils]
    val errorHandling = new ErrorHandling(mockS3Utils)
    val error = AssetProcessingError(
      consignmentId = None,
      matchId = None,
      source = None,
      errorCode = "ASSET_PROCESSING.TEST_ERROR",
      errorMsg = "Test error message"
    )

    val expectedErrorBytes = error.asJson.spaces2.getBytes(StandardCharsets.UTF_8)

    when(mockLogger.isErrorEnabled()).thenReturn(true)

    errorHandling.handleError(error, Logger(mockLogger))
    verify(mockLogger).error(
      "AssetProcessingError: consignmentId: None, matchId: None, source: None, errorCode: ASSET_PROCESSING.TEST_ERROR, errorMessage: Test error message"
    )

    val bucketCaptor = ArgumentCaptor.forClass(classOf[String])
    val bytesCaptor = ArgumentCaptor.forClass(classOf[Array[Byte]])
    val keyCaptor = ArgumentCaptor.forClass(classOf[String])
    verify(mockS3Utils, times(1)).putObject(bucketCaptor.capture(), bytesCaptor.capture(), keyCaptor.capture())

    assert(bucketCaptor.getValue.contains("transferErrorBucket"))
    assert(bytesCaptor.getValue sameElements expectedErrorBytes)
    assert(keyCaptor.getValue.contains("unknown/AssetProcessingError"))
    assert(keyCaptor.getValue.endsWith(".error"))
  }

  it should "log AggregateProcessingError messages and upload the error to s3" in {
    val mockLogger = mock[UnderlyingLogger]
    val mockS3Utils = mock[S3Utils]
    val consignmentId = java.util.UUID.randomUUID()
    val errorHandling = new ErrorHandling(mockS3Utils)
    val error = AggregateProcessingError(
      consignmentId = Some(consignmentId),
      errorCode = "AGGREGATE_PROCESSING.TEST_ERROR",
      errorMessage = "Test error message"
    )

    val expectedErrorBytes = error.asJson.spaces2.getBytes(StandardCharsets.UTF_8)

    when(mockLogger.isErrorEnabled()).thenReturn(true)

    errorHandling.handleError(error, Logger(mockLogger))
    verify(mockLogger).error(
      s"AggregateProcessingError: consignmentId: Some($consignmentId), errorCode: AGGREGATE_PROCESSING.TEST_ERROR, errorMessage: Test error message"
    )

    val bucketCaptor = ArgumentCaptor.forClass(classOf[String])
    val bytesCaptor = ArgumentCaptor.forClass(classOf[Array[Byte]])
    val keyCaptor = ArgumentCaptor.forClass(classOf[String])
    verify(mockS3Utils, times(1)).putObject(bucketCaptor.capture(), bytesCaptor.capture(), keyCaptor.capture())

    assert(bucketCaptor.getValue.contains("transferErrorBucket"))
    assert(bytesCaptor.getValue sameElements expectedErrorBytes)
    assert(keyCaptor.getValue.contains(s"$consignmentId/AggregateProcessingError"))
    assert(keyCaptor.getValue.endsWith(".error"))
  }

  it should "log TransferError messages and upload the error to s3" in {
    val mockLogger = mock[UnderlyingLogger]
    val mockS3Utils = mock[S3Utils]
    val consignmentId = java.util.UUID.randomUUID()
    val errorHandling = new ErrorHandling(mockS3Utils)
    val error = TransferError(
      consignmentId = Some(consignmentId),
      errorCode = "AGGREGATE_PROCESSING.TEST_ERROR",
      errorMessage = "Test error message"
    )

    val expectedErrorBytes = error.asJson.spaces2.getBytes(StandardCharsets.UTF_8)

    when(mockLogger.isErrorEnabled()).thenReturn(true)

    errorHandling.handleError(error, Logger(mockLogger))
    verify(mockLogger).error(
      s"TransferError: consignmentId: Some($consignmentId), errorCode: AGGREGATE_PROCESSING.TEST_ERROR, errorMessage: Test error message"
    )

    val bucketCaptor = ArgumentCaptor.forClass(classOf[String])
    val bytesCaptor = ArgumentCaptor.forClass(classOf[Array[Byte]])
    val keyCaptor = ArgumentCaptor.forClass(classOf[String])
    verify(mockS3Utils, times(1)).putObject(bucketCaptor.capture(), bytesCaptor.capture(), keyCaptor.capture())

    assert(bucketCaptor.getValue.contains("transferErrorBucket"))
    assert(bytesCaptor.getValue sameElements expectedErrorBytes)
    assert(keyCaptor.getValue.contains(s"$consignmentId/TransferError"))
    assert(keyCaptor.getValue.endsWith(".error"))
  }
}
