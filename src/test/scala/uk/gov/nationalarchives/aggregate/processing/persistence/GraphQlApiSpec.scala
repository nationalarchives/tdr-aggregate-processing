package uk.gov.nationalarchives.aggregate.processing.persistence

import cats.effect.unsafe.implicits.global
import com.nimbusds.oauth2.sdk.token.BearerAccessToken
import com.typesafe.scalalogging.Logger
import graphql.codegen.UpdateConsignmentStatus.{updateConsignmentStatus => ucs}
import graphql.codegen.types.ConsignmentStatusInput
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar.{doAnswer, mock, verify, when}
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.slf4j.{Logger => UnderlyingLogger}
import sangria.ast.Document
import sttp.client3.{HttpURLConnectionBackend, Identity, SttpBackend}
import uk.gov.nationalarchives.aggregate.processing.ExternalServiceSpec
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

  "updateConsignmentStatus" should "update the consignment status with the given arguments" in {
    val mockLogger = mock[UnderlyingLogger]

    val api = new GraphQlApi(keycloak, updateConsignmentStatusClient)(Logger(mockLogger), tdrKeycloakDeployment, backend)
    val consignmentId = UUID.randomUUID()
    val overrideUserId = UUID.randomUUID()

    when(mockLogger.isInfoEnabled()).thenReturn(true)

    doAnswer(() => Future(new BearerAccessToken("token")))
      .when(keycloak)
      .serviceAccountToken[Identity](any[String], any[String])(any[SttpBackend[Identity, Any]], any[ClassTag[Identity[_]]], any[TdrKeycloakDeployment])

    doAnswer(() => Future(GraphQlResponse[ucs.Data](Option(ucs.Data(Some(1))), Nil)))
      .when(updateConsignmentStatusClient)
      .getResult[Identity](any[BearerAccessToken], any[Document], any[Option[ucs.Variables]])(any[SttpBackend[Identity, Any]], any[ClassTag[Identity[_]]])

    val response = api.updateConsignmentStatus("client-secret", ConsignmentStatusInput(consignmentId, "Upload", Some("Completed"), Some(overrideUserId))).unsafeRunSync()
    response.get shouldBe 1

    verify(mockLogger).info(s"Updating consignment status: Upload for consignment: $consignmentId")
  }

  "updateConsignmentStatus" should "throw an error when fails to update" in {
    val consignmentId = UUID.randomUUID()
    val overrideUserId = UUID.randomUUID()
    val mockLogger = mock[UnderlyingLogger]

    doAnswer(() => Future(new BearerAccessToken("token")))
      .when(keycloak)
      .serviceAccountToken[Identity](any[String], any[String])(any[SttpBackend[Identity, Any]], any[ClassTag[Identity[_]]], any[TdrKeycloakDeployment])

    doAnswer(() => Future(GraphQlResponse[ucs.Data](None, Nil)))
      .when(updateConsignmentStatusClient)
      .getResult[Identity](any[BearerAccessToken], any[Document], any[Option[ucs.Variables]])(any[SttpBackend[Identity, Any]], any[ClassTag[Identity[_]]])

    val api = new GraphQlApi(keycloak, updateConsignmentStatusClient)(Logger(mockLogger), tdrKeycloakDeployment, backend)

    val exception = intercept[RuntimeException] {
      api.updateConsignmentStatus("client-secret", ConsignmentStatusInput(consignmentId, "Upload", Some("Completed"), Some(overrideUserId))).unsafeRunSync()
    }

    exception.getMessage shouldBe s"Unable to update consignment status: $consignmentId"
  }
}
