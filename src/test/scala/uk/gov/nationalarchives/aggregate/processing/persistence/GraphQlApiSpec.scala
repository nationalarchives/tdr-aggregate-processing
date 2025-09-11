package uk.gov.nationalarchives.aggregate.processing.persistence

import cats.effect.unsafe.implicits.global
import com.nimbusds.oauth2.sdk.token.BearerAccessToken
import com.typesafe.scalalogging.Logger
import graphql.codegen.AddFilesAndMetadata.{addFilesAndMetadata => afm}
import graphql.codegen.UpdateConsignmentStatus.{updateConsignmentStatus => ucs}
import graphql.codegen.types.{AddFileAndMetadataInput, ClientSideMetadataInput, ConsignmentStatusInput}
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar.{doAnswer, mock, times, verify, when}
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.slf4j.{Logger => UnderlyingLogger}
import sangria.ast.Document
import sttp.client3.{HttpURLConnectionBackend, Identity, SttpBackend}
import uk.gov.nationalarchives.aggregate.processing.ExternalServiceSpec
import uk.gov.nationalarchives.tdr.keycloak.{KeycloakUtils, TdrKeycloakDeployment}
import uk.gov.nationalarchives.tdr.{GraphQLClient, GraphQlResponse}
import uk.gov.nationalarchives.tdr.keycloak.{KeycloakUtils, TdrKeycloakDeployment}

import java.util.UUID
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.reflect.ClassTag

class GraphQlApiSpec extends ExternalServiceSpec {
  private val keycloak = mock[KeycloakUtils]

  implicit val tdrKeycloakDeployment: TdrKeycloakDeployment = TdrKeycloakDeployment("authUrl", "realm", 60)
  implicit val executionContext: ExecutionContextExecutor = scala.concurrent.ExecutionContext.global
  implicit val backend: SttpBackend[Identity, Any] = HttpURLConnectionBackend()

  private val updateConsignmentStatusClient = mock[GraphQLClient[ucs.Data, ucs.Variables]]
  private val addFilesAndMetadataClient = mock[GraphQLClient[afm.Data, afm.Variables]]

  "addClientSideMetadata" should "add client side metadata with the given arguments" in {
    val mockLogger = mock[UnderlyingLogger]
    val mockApiResponse = List[afm.AddFilesAndMetadata]()

    val api = new GraphQlApi(keycloak, updateConsignmentStatusClient, addFilesAndMetadataClient)(Logger(mockLogger), tdrKeycloakDeployment, backend)
    val consignmentId = UUID.randomUUID()
    val overrideUserId = UUID.randomUUID()
    val inputVariablesCaptor: ArgumentCaptor[Option[afm.Variables]] = ArgumentCaptor.forClass(classOf[Option[afm.Variables]])

    when(mockLogger.isInfoEnabled()).thenReturn(true)

    doAnswer(() => Future(new BearerAccessToken("token")))
      .when(keycloak)
      .serviceAccountToken[Identity](any[String], any[String])(any[SttpBackend[Identity, Any]], any[ClassTag[Identity[_]]], any[TdrKeycloakDeployment])

    doAnswer(() => Future(GraphQlResponse[afm.Data](Option(afm.Data(mockApiResponse)), Nil)))
      .when(addFilesAndMetadataClient)
      .getResult[Identity](any[BearerAccessToken], any[Document], inputVariablesCaptor.capture())(any[SttpBackend[Identity, Any]], any[ClassTag[Identity[_]]])

    val clientSideMetadataInput = List(ClientSideMetadataInput("original-path", "checksome", 1L, 2L, "match-id"))
    val input = AddFileAndMetadataInput(consignmentId, clientSideMetadataInput, None, Some(overrideUserId))

    api.addClientSideMetadata("client-secret", input).unsafeRunSync()
    val inputArgs = inputVariablesCaptor.getValue.get.input
    inputArgs.consignmentId shouldBe consignmentId
    inputArgs.metadataInput shouldBe clientSideMetadataInput
    inputArgs.userIdOverride.get shouldBe overrideUserId
    inputArgs.emptyDirectories shouldBe None

    verify(mockLogger).info("Add client side metadata for consignment: {}", consignmentId)
    verify(mockLogger, times(0)).isErrorEnabled
  }

  "addClientSideMetadata" should "throw an error when fails to update" in {
    val consignmentId = UUID.randomUUID()
    val overrideUserId = UUID.randomUUID()
    val mockLogger = mock[UnderlyingLogger]

    when(mockLogger.isInfoEnabled()).thenReturn(true)

    doAnswer(() => Future(new BearerAccessToken("token")))
      .when(keycloak)
      .serviceAccountToken[Identity](any[String], any[String])(any[SttpBackend[Identity, Any]], any[ClassTag[Identity[_]]], any[TdrKeycloakDeployment])

    doAnswer(() => Future(GraphQlResponse[afm.Data](None, Nil)))
      .when(addFilesAndMetadataClient)
      .getResult[Identity](any[BearerAccessToken], any[Document], any[Option[afm.Variables]])(any[SttpBackend[Identity, Any]], any[ClassTag[Identity[_]]])

    val input = AddFileAndMetadataInput(consignmentId, Nil, None, Some(overrideUserId))

    val api = new GraphQlApi(keycloak, updateConsignmentStatusClient, addFilesAndMetadataClient)(Logger(mockLogger), tdrKeycloakDeployment, backend)

    val exception = intercept[RuntimeException] {
      api.addClientSideMetadata("client-secret", input).unsafeRunSync()
    }

    verify(mockLogger).info("Add client side metadata for consignment: {}", consignmentId)
    exception.getMessage shouldBe s"Unable to add client side metadata: $consignmentId"
  }

  "updateConsignmentStatus" should "update the consignment status with the given arguments" in {
    val mockLogger = mock[UnderlyingLogger]

    val api = new GraphQlApi(keycloak, updateConsignmentStatusClient, addFilesAndMetadataClient)(Logger(mockLogger), tdrKeycloakDeployment, backend)
    val consignmentId = UUID.randomUUID()
    val overrideUserId = UUID.randomUUID()
    val inputVariablesCaptor: ArgumentCaptor[Option[ucs.Variables]] = ArgumentCaptor.forClass(classOf[Option[ucs.Variables]])

    when(mockLogger.isInfoEnabled()).thenReturn(true)

    doAnswer(() => Future(new BearerAccessToken("token")))
      .when(keycloak)
      .serviceAccountToken[Identity](any[String], any[String])(any[SttpBackend[Identity, Any]], any[ClassTag[Identity[_]]], any[TdrKeycloakDeployment])

    doAnswer(() => Future(GraphQlResponse[ucs.Data](Option(ucs.Data(Some(1))), Nil)))
      .when(updateConsignmentStatusClient)
      .getResult[Identity](any[BearerAccessToken], any[Document], inputVariablesCaptor.capture())(any[SttpBackend[Identity, Any]], any[ClassTag[Identity[_]]])

    api.updateConsignmentStatus("client-secret", ConsignmentStatusInput(consignmentId, "Upload", Some("Completed"), Some(overrideUserId))).unsafeRunSync()
    val inputArgs = inputVariablesCaptor.getValue.get.updateConsignmentStatusInput
    inputArgs.consignmentId shouldBe consignmentId
    inputArgs.statusType shouldBe "Upload"
    inputArgs.statusValue.get shouldBe "Completed"
    inputArgs.userIdOverride.get shouldBe overrideUserId

    verify(mockLogger).info(s"Updating consignment status: Upload for consignment: $consignmentId")
    verify(mockLogger, times(0)).isErrorEnabled
  }

  "updateConsignmentStatus" should "throw an error when fails to update" in {
    val consignmentId = UUID.randomUUID()
    val overrideUserId = UUID.randomUUID()
    val mockLogger = mock[UnderlyingLogger]

    when(mockLogger.isInfoEnabled()).thenReturn(true)

    doAnswer(() => Future(new BearerAccessToken("token")))
      .when(keycloak)
      .serviceAccountToken[Identity](any[String], any[String])(any[SttpBackend[Identity, Any]], any[ClassTag[Identity[_]]], any[TdrKeycloakDeployment])

    doAnswer(() => Future(GraphQlResponse[ucs.Data](None, Nil)))
      .when(updateConsignmentStatusClient)
      .getResult[Identity](any[BearerAccessToken], any[Document], any[Option[ucs.Variables]])(any[SttpBackend[Identity, Any]], any[ClassTag[Identity[_]]])

    val api = new GraphQlApi(keycloak, updateConsignmentStatusClient, addFilesAndMetadataClient)(Logger(mockLogger), tdrKeycloakDeployment, backend)

    val exception = intercept[RuntimeException] {
      api.updateConsignmentStatus("client-secret", ConsignmentStatusInput(consignmentId, "Upload", Some("Completed"), Some(overrideUserId))).unsafeRunSync()
    }

    verify(mockLogger).info(s"Updating consignment status: Upload for consignment: $consignmentId")
    exception.getMessage shouldBe s"Unable to update consignment status: $consignmentId"
  }
}
