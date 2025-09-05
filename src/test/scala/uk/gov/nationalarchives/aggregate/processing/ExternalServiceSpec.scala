package uk.gov.nationalarchives.aggregate.processing

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.stubbing.StubMapping
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

class ExternalServiceSpec extends AnyFlatSpec with BeforeAndAfterEach with BeforeAndAfterAll with ScalaFutures {
  override implicit def patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(5, Seconds)), interval = scaled(Span(100, Millis)))

  val wiremockSsmServer = new WireMockServer(9004)
  val wiremockS3 = new WireMockServer(8003)

  def setupSsmServer(): Unit = {
    wiremockSsmServer
      .stubFor(
        post(urlEqualTo("/"))
          .willReturn(okJson("{\"Parameter\":{\"Name\":\"string\",\"Value\":\"string\"}}"))
      )
  }

  override def beforeAll(): Unit = {
    wiremockS3.start()
    wiremockSsmServer.start()
  }

  override def afterAll(): Unit = {
    wiremockS3.stop()
    wiremockSsmServer.stop()
  }

  override def beforeEach(): Unit = {
    setupSsmServer()
  }

  override def afterEach(): Unit = {
    wiremockS3.resetAll()
    wiremockSsmServer.resetAll()
  }

  def mockS3GetResponse(): StubMapping = {
    wiremockS3.stubFor(WireMock.get(anyUrl()).willReturn(ok()))
  }
}
