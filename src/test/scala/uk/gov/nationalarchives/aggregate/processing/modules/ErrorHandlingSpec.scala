package uk.gov.nationalarchives.aggregate.processing.modules

import com.typesafe.scalalogging.Logger
import org.mockito.Mockito.verify
import org.mockito.MockitoSugar.{mock, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.{Logger => UnderlyingLogger}
import uk.gov.nationalarchives.aggregate.processing.modules.AssetProcessing.AssetProcessingError

class ErrorHandlingSpec extends AnyFlatSpec {
  "handleError" should "log the correct error message" in {
    val mockLogger = mock[UnderlyingLogger]
    val error = AssetProcessingError(
      consignmentId = Some("consignmentId123"),
      matchId = Some("matchId456"),
      source = Some("sourceSystem"),
      errorCode = "ASSET_PROCESSING.TEST_ERROR",
      errorMsg = "Test error message"
    )
    when(mockLogger.isErrorEnabled()).thenReturn(true)

    ErrorHandling.handleError(error, Logger(mockLogger))

    verify(mockLogger).error(
      "AssetProcessingError: consignmentId: consignmentId123, matchId: Some(matchId456), source: sourceSystem, errorCode: ASSET_PROCESSING.TEST_ERROR, errorMessage: Test error message"
    )
  }

  it should "log the error message even if matchId is None" in {
    val mockLogger = mock[UnderlyingLogger]
    val error = AssetProcessingError(
      consignmentId = None,
      matchId = None,
      source = None,
      errorCode = "ASSET_PROCESSING.TEST_ERROR",
      errorMsg = "Test error message"
    )
    when(mockLogger.isErrorEnabled()).thenReturn(true)

    ErrorHandling.handleError(error, Logger(mockLogger))
    verify(mockLogger).error(
      "AssetProcessingError: consignmentId: consignmentId123, matchId: None, source: sourceSystem, errorCode: ASSET_PROCESSING.TEST_ERROR, errorMessage: Test error message"
    )
  }
}
