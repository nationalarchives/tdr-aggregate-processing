package uk.gov.nationalarchives.aggregate.processing.modules

import cats.effect.IO
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
import uk.gov.nationalarchives.aggregate.processing.modules.Common.{AssetSource, ObjectCategory, ObjectKeyDetails}
import uk.gov.nationalarchives.aggregate.processing.modules.TransferOrchestration.{AssetProcessingEvent, BackendChecksStepFunctionInput}
import uk.gov.nationalarchives.aggregate.processing.persistence.GraphQlApi
import uk.gov.nationalarchives.aws.utils.stepfunction.StepFunctionUtils

import java.util.UUID

class TransferOrchestrationSpec extends ExternalServiceSpec {
  "orchestrate" should "update the consignment status correctly for asset processing event" in {
    val mockLogger = mock[UnderlyingLogger]
    val mockGraphQlApi = mock[GraphQlApi]
    val consignmentId = UUID.randomUUID()
    val userId = UUID.randomUUID()
    val objectKeyDetails = ObjectKeyDetails(userId, consignmentId, AssetSource.SharePoint, ObjectCategory.Metadata, objectElements = None)
    val sfnUtils = mock[StepFunctionUtils]
    val config = ConfigFactory.load()

    val inputCaptor: ArgumentCaptor[ConsignmentStatusInput] = ArgumentCaptor.forClass(classOf[ConsignmentStatusInput])

    when(mockLogger.isErrorEnabled()).thenReturn(true)
    when(mockGraphQlApi.updateConsignmentStatus(any[String], inputCaptor.capture())).thenReturn(IO(Some(1)))

    val event = AssetProcessingEvent(objectKeyDetails, processingErrors = false, suppliedMetadata = false)

    new TransferOrchestration(mockGraphQlApi, sfnUtils, config)(Logger(mockLogger)).orchestrate(event)

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
    val objectKeyDetails = ObjectKeyDetails(userId, consignmentId, AssetSource.SharePoint, ObjectCategory.Metadata, objectElements = None)
    val sfnUtils = mock[StepFunctionUtils]
    val config = ConfigFactory.load()

    val input: ArgumentCaptor[ConsignmentStatusInput] = ArgumentCaptor.forClass(classOf[ConsignmentStatusInput])

    when(mockLogger.isErrorEnabled()).thenReturn(true)
    when(mockGraphQlApi.updateConsignmentStatus(any[String], input.capture())).thenReturn(IO(Some(1)))

    val event = AssetProcessingEvent(objectKeyDetails, processingErrors = true, suppliedMetadata = false)

    new TransferOrchestration(mockGraphQlApi, sfnUtils, config)(Logger(mockLogger)).orchestrate(event)
    verify(mockLogger).error(
      s"TransferError: consignmentId: $consignmentId, errorCode: ASSET_PROCESSING.CompletedWithIssues, errorMessage: One or more assets failed to process."
    )

    input.getValue.consignmentId shouldBe consignmentId
    input.getValue.statusType shouldBe "Upload"
    input.getValue.statusValue.get shouldBe "Failed"
    input.getValue.userIdOverride.get shouldBe userId
  }

  "orchestrate" should "throw an error if the orchestration event is not of an expected class" in {
    case class SomeRandomClass()
    val mockLogger = mock[UnderlyingLogger]
    val mockGraphQlApi = mock[GraphQlApi]
    val sfnUtils = mock[StepFunctionUtils]
    val config = ConfigFactory.load()

    val orchestrator = new TransferOrchestration(mockGraphQlApi, sfnUtils, config)(Logger(mockLogger))

    val exception = intercept[RuntimeException] {
      orchestrator.orchestrate(SomeRandomClass())
    }

    exception.getMessage shouldEqual
      "Unrecognized orchestration event: uk.gov.nationalarchives.aggregate.processing.modules.TransferOrchestrationSpec$SomeRandomClass$1"
  }

  "orchestrate" should "trigger the backend checks step function for asset processing event" in {
    val mockLogger = mock[UnderlyingLogger]
    val mockGraphQlApi = mock[GraphQlApi]
    val consignmentId = UUID.randomUUID()
    val userId = UUID.randomUUID()
    val objectKeyDetails = ObjectKeyDetails(userId, consignmentId, AssetSource.SharePoint, ObjectCategory.Metadata, objectElements = None)
    val sfnUtils = mock[StepFunctionUtils]
    val config = ConfigFactory.load()
    val sfnResponse = IO.pure(StartExecutionResponse.builder.build)

    val arnCaptor: ArgumentCaptor[String] =
      ArgumentCaptor.forClass(classOf[String])
    val sfnInputCaptor: ArgumentCaptor[BackendChecksStepFunctionInput] =
      ArgumentCaptor.forClass(classOf[BackendChecksStepFunctionInput])
    val nameCaptor: ArgumentCaptor[Option[String]] =
      ArgumentCaptor.forClass(classOf[Option[String]])

    when(mockLogger.isInfoEnabled()).thenReturn(true)

    when(
      sfnUtils.startExecution(
        any[String],
        any[BackendChecksStepFunctionInput],
        any[Option[String]]
      )(any[Encoder[BackendChecksStepFunctionInput]])
    ).thenReturn(sfnResponse)

    val event = AssetProcessingEvent(objectKeyDetails, processingErrors = false, suppliedMetadata = false)

    new TransferOrchestration(mockGraphQlApi, sfnUtils, config)(Logger(mockLogger)).orchestrate(event)

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

    nameCaptor.getValue shouldBe Some(s"transfer_service_$consignmentId")
    verify(mockLogger).info(s"Triggering file checks for consignment: {}", consignmentId)
  }

  "orchestrate" should "not trigger the backend checks step function when asset processing contains errors" in {
    val mockLogger = mock[UnderlyingLogger]
    val mockGraphQlApi = mock[GraphQlApi]
    val consignmentId = UUID.randomUUID()
    val userId = UUID.randomUUID()
    val objectKeyDetails = ObjectKeyDetails(userId, consignmentId, AssetSource.SharePoint, ObjectCategory.Metadata, objectElements = None)
    val sfnUtils = mock[StepFunctionUtils]
    val config = ConfigFactory.load()

    val arnCaptor: ArgumentCaptor[String] =
      ArgumentCaptor.forClass(classOf[String])
    val sfnInputCaptor: ArgumentCaptor[BackendChecksStepFunctionInput] =
      ArgumentCaptor.forClass(classOf[BackendChecksStepFunctionInput])
    val nameCaptor: ArgumentCaptor[Option[String]] =
      ArgumentCaptor.forClass(classOf[Option[String]])

    when(mockLogger.isErrorEnabled()).thenReturn(true)

    val event = AssetProcessingEvent(objectKeyDetails, processingErrors = true, suppliedMetadata = false)

    new TransferOrchestration(mockGraphQlApi, sfnUtils, config)(Logger(mockLogger)).orchestrate(event)

    verify(sfnUtils, never).startExecution(
      arnCaptor.capture(),
      sfnInputCaptor.capture(),
      nameCaptor.capture()
    )(any[Encoder[BackendChecksStepFunctionInput]])

    verify(mockLogger).error(s"TransferError: consignmentId: $consignmentId, errorCode: ASSET_PROCESSING.CompletedWithIssues, errorMessage: One or more assets failed to process.")
  }
}
