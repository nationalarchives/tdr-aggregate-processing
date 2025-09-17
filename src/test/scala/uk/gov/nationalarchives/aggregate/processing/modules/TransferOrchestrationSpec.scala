package uk.gov.nationalarchives.aggregate.processing.modules

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.typesafe.scalalogging.Logger
import graphql.codegen.types.ConsignmentStatusInput
import org.mockito.ArgumentCaptor
import org.mockito.MockitoSugar.{mock, verify, when}
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.slf4j.{Logger => UnderlyingLogger}
import uk.gov.nationalarchives.aggregate.processing.ExternalServiceSpec
import uk.gov.nationalarchives.aggregate.processing.modules.TransferOrchestration.AggregateProcessingEvent
import uk.gov.nationalarchives.aggregate.processing.persistence.GraphQlApi

import java.util.UUID

class TransferOrchestrationSpec extends ExternalServiceSpec {
  "orchestrate" should "update the consignment status correctly for asset processing event" in {
    val mockLogger = mock[UnderlyingLogger]
    val mockGraphQlApi = mock[GraphQlApi]
    val consignmentId = UUID.randomUUID()
    val userId = UUID.randomUUID()

    val inputCaptor: ArgumentCaptor[ConsignmentStatusInput] = ArgumentCaptor.forClass(classOf[ConsignmentStatusInput])

    when(mockLogger.isErrorEnabled()).thenReturn(true)
    when(mockGraphQlApi.updateConsignmentStatus(inputCaptor.capture())).thenReturn(IO(Some(1)))

    val event = AggregateProcessingEvent(userId, consignmentId, processingErrors = false, suppliedMetadata = false)

    val result = new TransferOrchestration(mockGraphQlApi)(Logger(mockLogger)).orchestrate(event).unsafeRunSync()
    result.consignmentId.get shouldBe consignmentId
    result.success shouldBe true
    result.error shouldBe None

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
    when(mockGraphQlApi.updateConsignmentStatus(input.capture())).thenReturn(IO(Some(1)))

    val event = AggregateProcessingEvent(userId, consignmentId, processingErrors = true, suppliedMetadata = false)

    val result = new TransferOrchestration(mockGraphQlApi)(Logger(mockLogger)).orchestrate(event).unsafeRunSync()
    result.consignmentId.get shouldBe consignmentId
    result.success shouldBe true
    result.error shouldBe None

    verify(mockLogger).error(
      s"TransferError: consignmentId: Some($consignmentId), errorCode: AGGREGATE_PROCESSING.CompletedWithIssues, errorMessage: One or more assets failed to process."
    )

    input.getValue.consignmentId shouldBe consignmentId
    input.getValue.statusType shouldBe "Upload"
    input.getValue.statusValue.get shouldBe "Failed"
    input.getValue.userIdOverride.get shouldBe userId
  }

  "orchestrate" should "return a non-successful result when the orchestration event is not of an expected class" in {
    case class SomeRandomClass()
    val mockLogger = mock[UnderlyingLogger]
    val mockGraphQlApi = mock[GraphQlApi]

    when(mockLogger.isErrorEnabled()).thenReturn(true)

    val orchestrator = new TransferOrchestration(mockGraphQlApi)(Logger(mockLogger))

    val result = orchestrator.orchestrate(SomeRandomClass()).unsafeRunSync()
    result.consignmentId shouldBe None
    result.success shouldBe false
    result.error.get.consignmentId shouldBe None
    result.error.get.errorCode shouldBe "ORCHESTRATION.EVENT.INVALID"
    result.error.get.errorMessage shouldBe "Unrecognized orchestration event: uk.gov.nationalarchives.aggregate.processing.modules.TransferOrchestrationSpec$SomeRandomClass$1"

    verify(mockLogger).error(
      s"TransferError: consignmentId: None, errorCode: ORCHESTRATION.EVENT.INVALID, errorMessage: Unrecognized orchestration event: uk.gov.nationalarchives.aggregate.processing.modules.TransferOrchestrationSpec$$SomeRandomClass$$1"
    )
  }
}
