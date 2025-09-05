package uk.gov.nationalarchives.aggregate.processing.graphql

import cats.effect.IO
import cats.implicits.catsSyntaxOptionId
import com.typesafe.scalalogging.Logger
import graphql.codegen.UpdateConsignmentStatus.{updateConsignmentStatus => ucs}
import graphql.codegen.types.ConsignmentStatusInput
import sttp.client3.{HttpURLConnectionBackend, Identity, SttpBackend, SttpBackendOptions}
import uk.gov.nationalarchives.aggregate.processing.config.ApplicationConfig._
import uk.gov.nationalarchives.tdr.GraphQLClient
import uk.gov.nationalarchives.tdr.keycloak.{KeycloakUtils, TdrKeycloakDeployment}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class GraphQlApi(keycloak: KeycloakUtils, updateConsignmentStatusClient: GraphQLClient[ucs.Data, ucs.Variables])(implicit
    logger: Logger,
    keycloakDeployment: TdrKeycloakDeployment,
    backend: SttpBackend[Identity, Any]
) {
  def updateConsignmentStatus(clientSecret: String, consignmentStatusInput: ConsignmentStatusInput): IO[Option[Int]] = {
    val consignmentId = consignmentStatusInput.consignmentId

    logger.info("Updating consignment status: " + consignmentStatusInput.statusType + " for consignment: " + consignmentId)
    for {
      token <- keycloak.serviceAccountToken(authClientId, clientSecret).toIO
      result <- updateConsignmentStatusClient.getResult(token, ucs.document, ucs.Variables(consignmentStatusInput).some).toIO
      data <- IO.fromOption(result.data)(throw new RuntimeException(s"Unable to update consignment status: $consignmentId"))
    } yield data.updateConsignmentStatus
  }

  implicit class FutureUtils[T](f: Future[T]) {
    def toIO: IO[T] = IO.fromFuture(IO(f))
  }
}

object GraphQlApi {
  implicit val backend: SttpBackend[Identity, Any] = HttpURLConnectionBackend(options = SttpBackendOptions.connectionTimeout(graphqlApiRequestTimeOut))
  implicit val keycloakDeployment: TdrKeycloakDeployment = TdrKeycloakDeployment(authUrl, "tdr", timeToLiveSecs)
  private val keycloakUtils = new KeycloakUtils()
  private val updateConsignmentStatusClient = new GraphQLClient[ucs.Data, ucs.Variables](graphQlApiUrl)

  val logger = Logger[GraphQlApi]

  def apply()(implicit
      backend: SttpBackend[Identity, Any],
      keycloakDeployment: TdrKeycloakDeployment
  ) = new GraphQlApi(keycloakUtils, updateConsignmentStatusClient)(logger, keycloakDeployment, backend)
}
