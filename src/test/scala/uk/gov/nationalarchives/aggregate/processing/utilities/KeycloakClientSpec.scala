package uk.gov.nationalarchives.aggregate.processing.utilities

import com.typesafe.config.ConfigFactory
import org.mockito.ArgumentMatchers.any
import org.mockito.ArgumentMatchersSugar.eqTo
import org.mockito.Mockito._
import org.mockito.MockitoSugar.mock
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.tdr.keycloak.KeycloakUtils
import uk.gov.nationalarchives.tdr.keycloak.KeycloakUtils.UserDetails

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class KeycloakClientSpec extends AnyFlatSpec with Matchers with ScalaFutures {
  implicit val ec: ExecutionContext = ExecutionContext.global

  "KeycloakClient" should "return user details from KeycloakUtils" in {
    val userId = UUID.randomUUID().toString
    val config = ConfigFactory.load()
    val keycloakUtils = mock[KeycloakUtils]
    val expectedUserDetails = UserDetails("test@test.com")
    when(keycloakUtils.userDetails(eqTo(userId), any, any)(any, any, any)).thenReturn(Future.successful(expectedUserDetails))
    val keycloakClient = new KeycloakClient(keycloakUtils, config)

    val result = keycloakClient.userDetails(userId)
    whenReady(result) { userDetails =>
      userDetails shouldBe expectedUserDetails
    }
  }

  "KeycloakClient" should "fail when KeycloakUtils returns a failed Future" in {
    val userId = UUID.randomUUID().toString
    val config = ConfigFactory.load()
    val keycloakUtils = mock[KeycloakUtils]
    when(keycloakUtils.userDetails(eqTo(userId), any, any)(any, any, any)).thenReturn(Future.failed(new RuntimeException("Keycloak error")))
    val keycloakClient = new KeycloakClient(keycloakUtils, config)

    val exception = intercept[RuntimeException] {
      keycloakClient.userDetails(userId).futureValue
    }
    exception.getMessage shouldBe "The future returned an exception of type: java.lang.RuntimeException, with message: Keycloak error."
  }
}
