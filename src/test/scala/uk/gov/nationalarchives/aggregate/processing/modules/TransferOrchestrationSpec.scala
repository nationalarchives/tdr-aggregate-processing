package uk.gov.nationalarchives.aggregate.processing.modules

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.Logger
import graphql.codegen.types.ConsignmentStatusInput
import io.circe.Encoder
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar.{mock, never, verify, when}
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.slf4j.{Logger => UnderlyingLogger}
import software.amazon.awssdk.services.sfn.model.StartExecutionResponse
import uk.gov.nationalarchives.aggregate.processing.ExternalServiceSpec
import uk.gov.nationalarchives.aggregate.processing.modules.TransferOrchestration.{AggregateProcessingEvent, BackendChecksStepFunctionInput}
import uk.gov.nationalarchives.aggregate.processing.persistence.GraphQlApi
import uk.gov.nationalarchives.aws.utils.stepfunction.StepFunctionUtils

import java.util.UUID

class TransferOrchestrationSpec extends ExternalServiceSpec {
  "orchestrate" should "trigger backend processing and update the consignment status when asset processing event does not contain errors" in {
    val mockLogger = mock[UnderlyingLogger]
    val mockGraphQlApi = mock[GraphQlApi]
    val consignmentId = UUID.randomUUID()
    val userId = UUID.randomUUID()
    val sfnUtils = mock[StepFunctionUtils]
    val config = ConfigFactory.load()

    val consignmentStatusInputCaptor: ArgumentCaptor[ConsignmentStatusInput] = ArgumentCaptor.forClass(classOf[ConsignmentStatusInput])
    val sfnArnCaptor: ArgumentCaptor[String] = ArgumentCaptor.forClass(classOf[String])
    val sfnInputCaptor: ArgumentCaptor[BackendChecksStepFunctionInput] = ArgumentCaptor.forClass(classOf[BackendChecksStepFunctionInput])
    val sfnNameCaptor: ArgumentCaptor[Option[String]] = ArgumentCaptor.forClass(classOf[Option[String]])

    when(mockLogger.isInfoEnabled()).thenReturn(true)
    when(mockLogger.isErrorEnabled()).thenReturn(true)
    when(mockGraphQlApi.updateConsignmentStatus(any[ConsignmentStatusInput])).thenReturn(IO(Some(1)))
    when(sfnUtils.startExecution(any[String], any[BackendChecksStepFunctionInput], any[Option[String]])(any[Encoder[BackendChecksStepFunctionInput]]))
      .thenReturn(IO.pure(StartExecutionResponse.builder.build))

    val event = AggregateProcessingEvent(userId, consignmentId, processingErrors = false, suppliedMetadata = false)

    val result = new TransferOrchestration(mockGraphQlApi, sfnUtils, config)(Logger(mockLogger)).orchestrate(event).unsafeRunSync()
    result.consignmentId.get shouldBe consignmentId
    result.success shouldBe true
    result.error shouldBe None

    verify(mockLogger).info(s"Triggering file checks for consignment: {}", consignmentId)
    verify(mockLogger, never).isErrorEnabled()

    verify(mockGraphQlApi).updateConsignmentStatus(consignmentStatusInputCaptor.capture())
    consignmentStatusInputCaptor.getValue.consignmentId shouldBe consignmentId
    consignmentStatusInputCaptor.getValue.statusType shouldBe "Upload"
    consignmentStatusInputCaptor.getValue.statusValue.get shouldBe "Completed"
    consignmentStatusInputCaptor.getValue.userIdOverride.get shouldBe userId

    verify(sfnUtils).startExecution(sfnArnCaptor.capture(), sfnInputCaptor.capture(), sfnNameCaptor.capture())(any[Encoder[BackendChecksStepFunctionInput]])
    sfnArnCaptor.getValue shouldBe config.getString("sfn.backendChecksArn")
    sfnInputCaptor.getValue shouldBe BackendChecksStepFunctionInput(
      consignmentId.toString,
      s"$userId/sharepoint/$consignmentId/records"
    )
    sfnNameCaptor.getValue shouldBe Some(s"transfer_service_$consignmentId")
  }

  "orchestrate" should "log an error for asset processing event and update the consignment status correctly when asset processing contains errors" in {
    val mockLogger = mock[UnderlyingLogger]
    val mockGraphQlApi = mock[GraphQlApi]
    val consignmentId = UUID.randomUUID()
    val userId = UUID.randomUUID()
    val sfnUtils = mock[StepFunctionUtils]
    val config = ConfigFactory.load()

    val consignmentStatusInputCaptor: ArgumentCaptor[ConsignmentStatusInput] = ArgumentCaptor.forClass(classOf[ConsignmentStatusInput])

    when(mockLogger.isErrorEnabled()).thenReturn(true)
    when(mockGraphQlApi.updateConsignmentStatus(any[ConsignmentStatusInput])).thenReturn(IO(Some(1)))

    val event = AggregateProcessingEvent(userId, consignmentId, processingErrors = true, suppliedMetadata = false)

    val result = new TransferOrchestration(mockGraphQlApi, sfnUtils, config)(Logger(mockLogger)).orchestrate(event).unsafeRunSync()
    result.consignmentId.get shouldBe consignmentId
    result.success shouldBe true
    result.error shouldBe None

    verify(mockLogger).error(
      s"TransferError: consignmentId: Some($consignmentId), errorCode: AGGREGATE_PROCESSING.CompletedWithIssues, errorMessage: One or more assets failed to process."
    )

    verify(sfnUtils, never).startExecution(
      any[String],
      any[BackendChecksStepFunctionInput],
      any[Option[String]]
    )(any[Encoder[BackendChecksStepFunctionInput]])

    verify(mockGraphQlApi).updateConsignmentStatus(consignmentStatusInputCaptor.capture())
    consignmentStatusInputCaptor.getValue.consignmentId shouldBe consignmentId
    consignmentStatusInputCaptor.getValue.statusType shouldBe "Upload"
    consignmentStatusInputCaptor.getValue.statusValue.get shouldBe "Failed"
    consignmentStatusInputCaptor.getValue.userIdOverride.get shouldBe userId
  }

  "orchestrate" should "return a non-successful result when the orchestration event is not of an expected class" in {
    case class SomeRandomClass()
    val mockLogger = mock[UnderlyingLogger]
    val mockGraphQlApi = mock[GraphQlApi]
    val sfnUtils = mock[StepFunctionUtils]
    val config = ConfigFactory.load()

    when(mockLogger.isErrorEnabled()).thenReturn(true)

    val orchestrator = new TransferOrchestration(mockGraphQlApi, sfnUtils, config)(Logger(mockLogger))

    val result = orchestrator.orchestrate(SomeRandomClass()).unsafeRunSync()
    result.consignmentId shouldBe None
    result.success shouldBe false
    result.error.get.consignmentId shouldBe None
    result.error.get.errorCode shouldBe "ORCHESTRATION.EVENT.INVALID"
    result.error.get.errorMessage shouldBe "Unrecognized orchestration event: uk.gov.nationalarchives.aggregate.processing.modules.TransferOrchestrationSpec$SomeRandomClass$1"

    verify(sfnUtils, never).startExecution(
      any[String],
      any[BackendChecksStepFunctionInput],
      any[Option[String]]
    )(any[Encoder[BackendChecksStepFunctionInput]])

    verify(mockGraphQlApi, never).updateConsignmentStatus(any[ConsignmentStatusInput])

    verify(mockLogger).error(
      s"TransferError: consignmentId: None, errorCode: ORCHESTRATION.EVENT.INVALID, errorMessage: Unrecognized orchestration event: uk.gov.nationalarchives.aggregate.processing.modules.TransferOrchestrationSpec$$SomeRandomClass$$1"
    )
  }

  "orchestrate" should "log an error when triggering the backend checks step function fails" in {
    val mockLogger = mock[UnderlyingLogger]
    val mockGraphQlApi = mock[GraphQlApi]
    val consignmentId = UUID.randomUUID()
    val userId = UUID.randomUUID()
    val sfnUtils = mock[StepFunctionUtils]
    val config = ConfigFactory.load()

    val arnCaptor: ArgumentCaptor[String] =
      ArgumentCaptor.forClass(classOf[String])
    val sfnInputCaptor: ArgumentCaptor[BackendChecksStepFunctionInput] =
      ArgumentCaptor.forClass(classOf[BackendChecksStepFunctionInput])
    val nameCaptor: ArgumentCaptor[Option[String]] =
      ArgumentCaptor.forClass(classOf[Option[String]])
    val consignmentStatusInputCaptor: ArgumentCaptor[ConsignmentStatusInput] = ArgumentCaptor.forClass(classOf[ConsignmentStatusInput])

    when(mockLogger.isErrorEnabled()).thenReturn(true)
    when(mockGraphQlApi.updateConsignmentStatus(any[ConsignmentStatusInput])).thenReturn(IO(Some(1)))

    when(
      sfnUtils.startExecution(
        any[String],
        any[BackendChecksStepFunctionInput],
        any[Option[String]]
      )(any[Encoder[BackendChecksStepFunctionInput]])
    ).thenReturn(IO.raiseError(new RuntimeException("Step function failed")))

    val event = AggregateProcessingEvent(userId, consignmentId, processingErrors = false, suppliedMetadata = false)

    new TransferOrchestration(mockGraphQlApi, sfnUtils, config)(Logger(mockLogger)).orchestrate(event).unsafeRunSync()

    verify(sfnUtils).startExecution(
      arnCaptor.capture(),
      sfnInputCaptor.capture(),
      nameCaptor.capture()
    )(any[Encoder[BackendChecksStepFunctionInput]])

    arnCaptor.getValue shouldBe config.getString("sfn.backendChecksArn")

    sfnInputCaptor.getValue shouldBe BackendChecksStepFunctionInput(
      consignmentId.toString,
      s"$userId/sharepoint/$consignmentId/metadata"
    )

    verify(mockGraphQlApi).updateConsignmentStatus(consignmentStatusInputCaptor.capture())
    nameCaptor.getValue shouldBe Some(s"transfer_service_$consignmentId")
    verify(mockLogger).error(s"Step function failed")
  }
}
