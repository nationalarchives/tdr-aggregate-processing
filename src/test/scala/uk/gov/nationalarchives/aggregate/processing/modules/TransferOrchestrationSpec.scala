package uk.gov.nationalarchives.aggregate.processing.modules

import cats.effect.IO
import com.typesafe.scalalogging.Logger
import graphql.codegen.types.ConsignmentStatusInput
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar.{mock, verify, when}
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.slf4j.{Logger => UnderlyingLogger}
import uk.gov.nationalarchives.aggregate.processing.ExternalServiceSpec
import uk.gov.nationalarchives.aggregate.processing.graphql.GraphQlApi
import uk.gov.nationalarchives.aggregate.processing.modules.TransferOrchestration.AssetProcessingEvent

import java.util.UUID

class TransferOrchestrationSpec extends ExternalServiceSpec {
  "orchestrate" should "update the consignment status correctly for asset processing event" in {
    val mockLogger = mock[UnderlyingLogger]
    val mockGraphQlApi = mock[GraphQlApi]
    val consignmentId = UUID.randomUUID()
    val userId = UUID.randomUUID()

    val inputCaptor: ArgumentCaptor[ConsignmentStatusInput] = ArgumentCaptor.forClass(classOf[ConsignmentStatusInput])

    when(mockLogger.isErrorEnabled()).thenReturn(true)
    when(mockGraphQlApi.updateConsignmentStatus(any[String], inputCaptor.capture())).thenReturn(IO(Some(1)))

    val event = AssetProcessingEvent(userId, consignmentId, processingErrors = false, suppliedMetadata = false)

    new TransferOrchestration(mockGraphQlApi)(Logger(mockLogger)).orchestrate(event)

    inputCaptor.getValue.consignmentId shouldBe consignmentId
    inputCaptor.getValue.statusType shouldBe "Upload"
    inputCaptor.getValue.statusValue.get shouldBe "Completed"
    inputCaptor.getValue.userIdOverride.get shouldBe userId
  }


  "orchestrate" should "log an error for asset processing event when asset processing contains errors and update the consignment status correctly" in {
    val mockLogger = mock[UnderlyingLogger]
    val mockGraphQlApi = mock[GraphQlApi]
    val consignmentId = UUID.randomUUID()
    val userId = UUID.randomUUID()

    val input: ArgumentCaptor[ConsignmentStatusInput] = ArgumentCaptor.forClass(classOf[ConsignmentStatusInput])

    when(mockLogger.isErrorEnabled()).thenReturn(true)
    when(mockGraphQlApi.updateConsignmentStatus(any[String], input.capture())).thenReturn(IO(Some(1)))

    val event = AssetProcessingEvent(userId, consignmentId, processingErrors = true, suppliedMetadata = false)

    new TransferOrchestration(mockGraphQlApi)(Logger(mockLogger)).orchestrate(event)
    verify(mockLogger).error(
      s"TransferError$$: consignmentId: $consignmentId, errorCode: ASSET_PROCESSING.completedWithIssues, errorMessage: One or more assets failed to process.")

    input.getValue.consignmentId shouldBe consignmentId
    input.getValue.statusType shouldBe "Upload"
    input.getValue.statusValue.get shouldBe "Failed"
    input.getValue.userIdOverride.get shouldBe userId
  }

  "orchestrate" should "throw an error if the orchestration event is not of an expected class" in {
    case class SomeRandomClass()
    val mockLogger = mock[UnderlyingLogger]
    val mockGraphQlApi = mock[GraphQlApi]

    val orchestrator = new TransferOrchestration(mockGraphQlApi)(Logger(mockLogger))

    val exception = intercept[RuntimeException] {
      orchestrator.orchestrate(SomeRandomClass())
    }

    exception.getMessage shouldEqual
      "Unrecognized orchestration event: uk.gov.nationalarchives.aggregate.processing.modules.TransferOrchestrationSpec$SomeRandomClass$1"
  }
}
