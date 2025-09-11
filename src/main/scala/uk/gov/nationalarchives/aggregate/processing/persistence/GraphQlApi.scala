package uk.gov.nationalarchives.aggregate.processing.persistence

import cats.effect.IO
import cats.implicits.catsSyntaxOptionId
import com.typesafe.scalalogging.Logger
import graphql.codegen.AddFilesAndMetadata.{addFilesAndMetadata => afm}
import graphql.codegen.UpdateConsignmentStatus.{updateConsignmentStatus => ucs}
import graphql.codegen.types.{AddFileAndMetadataInput, ConsignmentStatusInput}
import sttp.client3.{HttpURLConnectionBackend, Identity, SttpBackend, SttpBackendOptions}
import uk.gov.nationalarchives.aggregate.processing.config.ApplicationConfig._
import uk.gov.nationalarchives.tdr.GraphQLClient
import uk.gov.nationalarchives.tdr.keycloak.{KeycloakUtils, TdrKeycloakDeployment}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class GraphQlApi(keycloak: KeycloakUtils, updateConsignmentStatusClient: GraphQLClient[ucs.Data, ucs.Variables], addFilesAndMetadataClient: GraphQLClient[afm.Data, afm.Variables])(
    implicit
    logger: Logger,
    keycloakDeployment: TdrKeycloakDeployment,
    backend: SttpBackend[Identity, Any]
) {
  implicit class FutureUtils[T](f: Future[T]) {
    def toIO: IO[T] = IO.fromFuture(IO(f))
  }

  def updateConsignmentStatus(clientSecret: String, consignmentStatusInput: ConsignmentStatusInput): IO[Option[Int]] = {
    val consignmentId = consignmentStatusInput.consignmentId

    logger.info("Updating consignment status: " + consignmentStatusInput.statusType + " for consignment: " + consignmentId)
    for {
      token <- keycloak.serviceAccountToken(authClientId, clientSecret).toIO
      result <- updateConsignmentStatusClient.getResult(token, ucs.document, ucs.Variables(consignmentStatusInput).some).toIO
      data <- IO.fromOption(result.data)(throw new RuntimeException(s"Unable to update consignment status: $consignmentId"))
    } yield data.updateConsignmentStatus
  }

  def addClientSideMetadata(clientSecret: String, addFileAndMetadataInput: AddFileAndMetadataInput): IO[List[afm.AddFilesAndMetadata]] = {
    val consignmentId = addFileAndMetadataInput.consignmentId

    logger.info(s"Add client side metadata for consignment: $consignmentId")
    for {
      token <- keycloak.serviceAccountToken(authClientId, clientSecret).toIO
      result <- addFilesAndMetadataClient.getResult(token, afm.document, afm.Variables(addFileAndMetadataInput).some).toIO
      data <- IO.fromOption(result.data)(throw new RuntimeException(s"Unable to add client side metadata: $consignmentId"))
    } yield data.addFilesAndMetadata
  }
}

object GraphQlApi {
  implicit val backend: SttpBackend[Identity, Any] = HttpURLConnectionBackend(options = SttpBackendOptions.connectionTimeout(graphqlApiRequestTimeOut))
  implicit val keycloakDeployment: TdrKeycloakDeployment = TdrKeycloakDeployment(authUrl, "tdr", timeToLiveSecs)
  private val keycloakUtils = new KeycloakUtils()
  private val updateConsignmentStatusClient = new GraphQLClient[ucs.Data, ucs.Variables](graphQlApiUrl)
  private val addFilesAndMetadataClient = new GraphQLClient[afm.Data, afm.Variables](graphQlApiUrl)

  val logger = Logger[GraphQlApi]

  def apply()(implicit
      backend: SttpBackend[Identity, Any],
      keycloakDeployment: TdrKeycloakDeployment
  ) = new GraphQlApi(keycloakUtils, updateConsignmentStatusClient, addFilesAndMetadataClient)(logger, keycloakDeployment, backend)
}
