package uk.gov.nationalarchives.aggregate.processing.modules

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.Logger
import graphql.codegen.GetConsignment.getConsignment
import graphql.codegen.types.ConsignmentStatusInput
import io.circe.Encoder
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar.{mock, never, verify, when}
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.slf4j.{Logger => UnderlyingLogger}
import software.amazon.awssdk.services.sfn.model.StartExecutionResponse
import software.amazon.awssdk.services.sns.model.PublishResponse
import uk.gov.nationalarchives.aggregate.processing.ExternalServiceSpec
import uk.gov.nationalarchives.aggregate.processing.modules.TransferOrchestration.{AggregateProcessingEvent, BackendChecksStepFunctionInput}
import uk.gov.nationalarchives.aggregate.processing.persistence.GraphQlApi
import uk.gov.nationalarchives.aggregate.processing.utilities.NotificationsClient.UploadEvent
import uk.gov.nationalarchives.aggregate.processing.utilities.{KeycloakClient, NotificationsClient}
import uk.gov.nationalarchives.aws.utils.stepfunction.StepFunctionUtils
import uk.gov.nationalarchives.tdr.keycloak.KeycloakUtils.UserDetails

import java.util.UUID
import scala.concurrent.Future

class TransferOrchestrationSpec extends ExternalServiceSpec {
  val consignmentId: UUID = UUID.randomUUID()
  val userId: UUID = UUID.randomUUID()
  val userEmail = "test@test.com"
  val transferringBody = "transferringBody"
  val consignmentRef = "consignmentRef"
  val consignmentDetailsResponseStub = IO(
    Some(
      getConsignment.GetConsignment(
        userId,
        None,
        None,
        consignmentRef,
        None,
        None,
        Some(transferringBody),
        Nil
      )
    )
  )

  "orchestrate" should "trigger backend processing, update the consignment status and send an upload complete sns message when asset processing event does not contain errors" in {
    val mockLogger = mock[UnderlyingLogger]
    val mockGraphQlApi = mock[GraphQlApi]
    val sfnUtils = mock[StepFunctionUtils]
    val notificationUtils = mock[NotificationsClient]
    val keycloakConfigurations = mock[KeycloakClient]
    val config = ConfigFactory.load()

    val consignmentStatusInputCaptor: ArgumentCaptor[ConsignmentStatusInput] = ArgumentCaptor.forClass(classOf[ConsignmentStatusInput])
    val sfnArnCaptor: ArgumentCaptor[String] = ArgumentCaptor.forClass(classOf[String])
    val sfnInputCaptor: ArgumentCaptor[BackendChecksStepFunctionInput] = ArgumentCaptor.forClass(classOf[BackendChecksStepFunctionInput])
    val sfnNameCaptor: ArgumentCaptor[Option[String]] = ArgumentCaptor.forClass(classOf[Option[String]])
    val snsArgCaptor: ArgumentCaptor[UploadEvent] = ArgumentCaptor.forClass(classOf[UploadEvent])

    when(mockLogger.isInfoEnabled()).thenReturn(true)
    when(mockLogger.isErrorEnabled()).thenReturn(true)
    when(mockGraphQlApi.updateConsignmentStatus(any[ConsignmentStatusInput])).thenReturn(IO(Some(1)))
    when(mockGraphQlApi.getConsignmentDetails(consignmentId)).thenReturn(consignmentDetailsResponseStub)
    when(sfnUtils.startExecution(any[String], any[BackendChecksStepFunctionInput], any[Option[String]])(any[Encoder[BackendChecksStepFunctionInput]]))
      .thenReturn(IO.pure(StartExecutionResponse.builder.build))
    when(keycloakConfigurations.userDetails(userId.toString)).thenReturn(Future.successful(UserDetails(userEmail)))
    when(notificationUtils.publishUploadEvent(any[NotificationsClient.UploadEvent])).thenReturn(IO.pure(PublishResponse.builder.build()))

    val event = AggregateProcessingEvent(userId, consignmentId, processingErrors = false, suppliedMetadata = false)

    val result = new TransferOrchestration(mockGraphQlApi, sfnUtils, notificationUtils, keycloakConfigurations, config)(Logger(mockLogger)).orchestrate(event).unsafeRunSync()
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

    verify(mockGraphQlApi).getConsignmentDetails(consignmentId)

    verify(keycloakConfigurations).userDetails(userId.toString)
    verify(sfnUtils).startExecution(sfnArnCaptor.capture(), sfnInputCaptor.capture(), sfnNameCaptor.capture())(any[Encoder[BackendChecksStepFunctionInput]])
    sfnArnCaptor.getValue shouldBe config.getString("sfn.backendChecksArn")
    sfnInputCaptor.getValue shouldBe BackendChecksStepFunctionInput(
      consignmentId.toString,
      s"$userId/sharepoint/$consignmentId/records"
    )
    sfnNameCaptor.getValue shouldBe Some(s"transfer_service_$consignmentId")

    verify(notificationUtils).publishUploadEvent(snsArgCaptor.capture())
    snsArgCaptor.getValue.userEmail shouldBe userEmail
    snsArgCaptor.getValue.userId shouldBe userId.toString
    snsArgCaptor.getValue.consignmentId shouldBe consignmentId.toString
    snsArgCaptor.getValue.status shouldBe "Completed"
    snsArgCaptor.getValue.transferringBodyName shouldBe transferringBody
    snsArgCaptor.getValue.consignmentReference shouldBe consignmentRef
  }

  "orchestrate" should "log an error for asset processing event, update the consignment status correctly and send a upload failed sns message when asset processing contains errors" in {
    val mockLogger = mock[UnderlyingLogger]
    val mockGraphQlApi = mock[GraphQlApi]
    val sfnUtils = mock[StepFunctionUtils]
    val notificationUtils = mock[NotificationsClient]
    val keycloakConfigurations = mock[KeycloakClient]
    val config = ConfigFactory.load()

    val consignmentStatusInputCaptor: ArgumentCaptor[ConsignmentStatusInput] = ArgumentCaptor.forClass(classOf[ConsignmentStatusInput])
    val snsArgCaptor: ArgumentCaptor[UploadEvent] = ArgumentCaptor.forClass(classOf[UploadEvent])

    when(mockLogger.isErrorEnabled()).thenReturn(true)
    when(mockGraphQlApi.updateConsignmentStatus(any[ConsignmentStatusInput])).thenReturn(IO(Some(1)))
    when(mockGraphQlApi.getConsignmentDetails(consignmentId)).thenReturn(consignmentDetailsResponseStub)
    when(keycloakConfigurations.userDetails(userId.toString)).thenReturn(Future.successful(UserDetails(userEmail)))
    when(notificationUtils.publishUploadEvent(any[NotificationsClient.UploadEvent])).thenReturn(IO.pure(PublishResponse.builder.build()))

    val event = AggregateProcessingEvent(userId, consignmentId, processingErrors = true, suppliedMetadata = false)

    val result = new TransferOrchestration(mockGraphQlApi, sfnUtils, notificationUtils, keycloakConfigurations, config)(Logger(mockLogger)).orchestrate(event).unsafeRunSync()
    result.consignmentId.get shouldBe consignmentId
    result.success shouldBe true
    result.error shouldBe None

    verify(mockLogger).error(
      s"TransferError: consignmentId: Some($consignmentId), errorCode: AGGREGATE_PROCESSING.CompletedWithIssues, errorMessage: One or more assets failed to process."
    )

    verify(keycloakConfigurations).userDetails(userId.toString)
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

    verify(mockGraphQlApi).getConsignmentDetails(consignmentId)

    verify(notificationUtils).publishUploadEvent(snsArgCaptor.capture())
    snsArgCaptor.getValue.userEmail shouldBe userEmail
    snsArgCaptor.getValue.userId shouldBe userId.toString
    snsArgCaptor.getValue.consignmentId shouldBe consignmentId.toString
    snsArgCaptor.getValue.status shouldBe "Failed"
    snsArgCaptor.getValue.transferringBodyName shouldBe transferringBody
    snsArgCaptor.getValue.consignmentReference shouldBe consignmentRef
  }

  "orchestrate" should "return a non-successful result when the orchestration event is not of an expected class" in {
    case class SomeRandomClass()
    val mockLogger = mock[UnderlyingLogger]
    val mockGraphQlApi = mock[GraphQlApi]
    val sfnUtils = mock[StepFunctionUtils]
    val notificationUtils = mock[NotificationsClient]
    val keycloakConfigurations = mock[KeycloakClient]
    val config = ConfigFactory.load()

    when(mockLogger.isErrorEnabled()).thenReturn(true)

    val orchestrator = new TransferOrchestration(mockGraphQlApi, sfnUtils, notificationUtils, keycloakConfigurations, config)(Logger(mockLogger))

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
    val sfnUtils = mock[StepFunctionUtils]
    val notificationUtils = mock[NotificationsClient]
    val keycloakConfigurations = mock[KeycloakClient]
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
    when(mockGraphQlApi.getConsignmentDetails(consignmentId)).thenReturn(consignmentDetailsResponseStub)
    when(keycloakConfigurations.userDetails(userId.toString)).thenReturn(Future.successful(UserDetails(userEmail)))

    when(
      sfnUtils.startExecution(
        any[String],
        any[BackendChecksStepFunctionInput],
        any[Option[String]]
      )(any[Encoder[BackendChecksStepFunctionInput]])
    ).thenReturn(IO.raiseError(new RuntimeException("Step function failed")))

    when(notificationUtils.publishUploadEvent(any[NotificationsClient.UploadEvent])).thenReturn(IO.pure(PublishResponse.builder.build()))

    val event = AggregateProcessingEvent(userId, consignmentId, processingErrors = false, suppliedMetadata = false)

    new TransferOrchestration(mockGraphQlApi, sfnUtils, notificationUtils, keycloakConfigurations, config)(Logger(mockLogger)).orchestrate(event).unsafeRunSync()

    verify(keycloakConfigurations).userDetails(userId.toString)
    verify(sfnUtils).startExecution(
      arnCaptor.capture(),
      sfnInputCaptor.capture(),
      nameCaptor.capture()
    )(any[Encoder[BackendChecksStepFunctionInput]])

    arnCaptor.getValue shouldBe config.getString("sfn.backendChecksArn")

    sfnInputCaptor.getValue shouldBe BackendChecksStepFunctionInput(
      consignmentId.toString,
      s"$userId/sharepoint/$consignmentId/records"
    )

    verify(mockGraphQlApi).updateConsignmentStatus(consignmentStatusInputCaptor.capture())
    verify(mockGraphQlApi).getConsignmentDetails(consignmentId)
    nameCaptor.getValue shouldBe Some(s"transfer_service_$consignmentId")
    verify(mockLogger).error(s"Step function failed")
  }
}
