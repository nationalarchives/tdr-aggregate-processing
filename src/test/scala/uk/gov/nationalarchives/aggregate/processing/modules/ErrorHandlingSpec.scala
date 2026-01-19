package uk.gov.nationalarchives.aggregate.processing.modules

import com.typesafe.scalalogging.Logger
import org.mockito.ArgumentCaptor
import org.mockito.Mockito.{times, verify}
import org.mockito.MockitoSugar.{mock, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.{Logger => UnderlyingLogger}
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import uk.gov.nationalarchives.aggregate.processing.AggregateProcessingLambda.AggregateProcessingError
import uk.gov.nationalarchives.aggregate.processing.modules.assetprocessing.AssetProcessing.AssetProcessingError
import uk.gov.nationalarchives.aggregate.processing.modules.orchestration.TransferOrchestration.TransferError

class ErrorHandlingSpec extends AnyFlatSpec {
  "handleError" should "log AssetProcessingError messages and upload the error to s3" in {
    val mockLogger = mock[UnderlyingLogger]
    val mockS3AsyncClient = mock[S3AsyncClient]
    val consignmentId = java.util.UUID.randomUUID()
    val errorHandling = new ErrorHandling(mockS3AsyncClient)
    val error = AssetProcessingError(
      consignmentId = Some(consignmentId),
      matchId = Some("matchId456"),
      source = Some("sourceSystem"),
      errorCode = "ASSET_PROCESSING.TEST_ERROR",
      errorMsg = "Test error message"
    )
    when(mockLogger.isErrorEnabled()).thenReturn(true)

    errorHandling.handleError(error, Logger(mockLogger))

    verify(mockLogger).error(
      s"AssetProcessingError: consignmentId: Some($consignmentId), matchId: Some(matchId456), source: Some(sourceSystem), errorCode: ASSET_PROCESSING.TEST_ERROR, errorMessage: Test error message"
    )

    val putObjectRequestCaptor = ArgumentCaptor.forClass(classOf[PutObjectRequest])
    val asyncRequestBodyCaptor = ArgumentCaptor.forClass(classOf[AsyncRequestBody])
    verify(mockS3AsyncClient, times(1)).putObject(putObjectRequestCaptor.capture(), asyncRequestBodyCaptor.capture())

    val putObjectRequest = putObjectRequestCaptor.getValue
    assert(putObjectRequest.bucket().contains("transferErrorBucket"))
    assert(putObjectRequest.key().contains(s"$consignmentId/AssetProcessingError"))
    assert(putObjectRequest.key().endsWith(".error"))
  }

  it should "log an error when optional fields are None and upload error to s3" in {
    val mockLogger = mock[UnderlyingLogger]
    val mockS3AsyncClient = mock[S3AsyncClient]
    val errorHandling = new ErrorHandling(mockS3AsyncClient)
    val error = AssetProcessingError(
      consignmentId = None,
      matchId = None,
      source = None,
      errorCode = "ASSET_PROCESSING.TEST_ERROR",
      errorMsg = "Test error message"
    )
    when(mockLogger.isErrorEnabled()).thenReturn(true)

    errorHandling.handleError(error, Logger(mockLogger))
    verify(mockLogger).error(
      "AssetProcessingError: consignmentId: None, matchId: None, source: None, errorCode: ASSET_PROCESSING.TEST_ERROR, errorMessage: Test error message"
    )

    val putObjectRequestCaptor = ArgumentCaptor.forClass(classOf[PutObjectRequest])
    val asyncRequestBodyCaptor = ArgumentCaptor.forClass(classOf[AsyncRequestBody])
    verify(mockS3AsyncClient, times(1)).putObject(putObjectRequestCaptor.capture(), asyncRequestBodyCaptor.capture())
    val putObjectRequest = putObjectRequestCaptor.getValue
    assert(putObjectRequest.bucket().contains("transferErrorBucket"))
    assert(putObjectRequest.key().contains("unknown/AssetProcessingError"))
    assert(putObjectRequest.key().endsWith(".error"))
  }

  it should "log AggregateProcessingError messages and upload the error to s3" in {
    val mockLogger = mock[UnderlyingLogger]
    val mockS3AsyncClient = mock[S3AsyncClient]
    val consignmentId = java.util.UUID.randomUUID()
    val errorHandling = new ErrorHandling(mockS3AsyncClient)
    val error = AggregateProcessingError(
      consignmentId = Some(consignmentId),
      errorCode = "AGGREGATE_PROCESSING.TEST_ERROR",
      errorMessage = "Test error message"
    )
    when(mockLogger.isErrorEnabled()).thenReturn(true)

    errorHandling.handleError(error, Logger(mockLogger))
    verify(mockLogger).error(
      s"AggregateProcessingError: consignmentId: Some($consignmentId), errorCode: AGGREGATE_PROCESSING.TEST_ERROR, errorMessage: Test error message"
    )

    val putObjectRequestCaptor = ArgumentCaptor.forClass(classOf[PutObjectRequest])
    val asyncRequestBodyCaptor = ArgumentCaptor.forClass(classOf[AsyncRequestBody])
    verify(mockS3AsyncClient, times(1)).putObject(putObjectRequestCaptor.capture(), asyncRequestBodyCaptor.capture())
    val putObjectRequest = putObjectRequestCaptor.getValue
    assert(putObjectRequest.bucket().contains("transferErrorBucket"))
    assert(putObjectRequest.key().contains(s"$consignmentId/AggregateProcessingError"))
    assert(putObjectRequest.key().endsWith(".error"))
  }

  it should "log TransferError messages and upload the error to s3" in {
    val mockLogger = mock[UnderlyingLogger]
    val mockS3AsyncClient = mock[S3AsyncClient]
    val consignmentId = java.util.UUID.randomUUID()
    val errorHandling = new ErrorHandling(mockS3AsyncClient)
    val error = TransferError(
      consignmentId = Some(consignmentId),
      errorCode = "AGGREGATE_PROCESSING.TEST_ERROR",
      errorMessage = "Test error message"
    )
    when(mockLogger.isErrorEnabled()).thenReturn(true)

    errorHandling.handleError(error, Logger(mockLogger))
    verify(mockLogger).error(
      s"TransferError: consignmentId: Some($consignmentId), errorCode: AGGREGATE_PROCESSING.TEST_ERROR, errorMessage: Test error message"
    )

    val putObjectRequestCaptor = ArgumentCaptor.forClass(classOf[PutObjectRequest])
    val asyncRequestBodyCaptor = ArgumentCaptor.forClass(classOf[AsyncRequestBody])
    verify(mockS3AsyncClient, times(1)).putObject(putObjectRequestCaptor.capture(), asyncRequestBodyCaptor.capture())
    val putObjectRequest = putObjectRequestCaptor.getValue
    assert(putObjectRequest.bucket().contains("transferErrorBucket"))
    assert(putObjectRequest.key().contains(s"$consignmentId/TransferError"))
    assert(putObjectRequest.key().endsWith(".error"))
  }
}
