package uk.gov.nationalarchives.aggregate.processing.utilities

import com.typesafe.config.Config
import sttp.client3.{HttpURLConnectionBackend, Identity, SttpBackend}
import uk.gov.nationalarchives.tdr.keycloak.KeycloakUtils.UserDetails
import uk.gov.nationalarchives.tdr.keycloak.{KeycloakUtils, TdrKeycloakDeployment}

import scala.concurrent.{ExecutionContext, Future}

class KeycloakConfigurations(config: Config)(implicit val executionContext: ExecutionContext) {
  implicit val backend: SttpBackend[Identity, Any] = HttpURLConnectionBackend()
  val authUrl: String = config.getString("auth.url")
  val secret: String = config.getString("read.auth.secret.path")

  def userDetails(userId: String): Future[UserDetails] = {
    implicit val tdrKeycloakDeployment: TdrKeycloakDeployment =
      TdrKeycloakDeployment(authUrl, "tdr", 3600)
    KeycloakUtils().userDetails(userId, "tdr-user-read", secret)
  }
}

object KeycloakConfigurations {

  def apply(config: Config)(implicit executionContext: ExecutionContext): KeycloakConfigurations = new KeycloakConfigurations(config)
}
