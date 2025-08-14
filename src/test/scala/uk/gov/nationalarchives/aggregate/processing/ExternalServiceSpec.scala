package uk.gov.nationalarchives.aggregate.processing

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock
import com.github.tomakehurst.wiremock.client.WireMock.{anyUrl, ok, put}
import com.github.tomakehurst.wiremock.stubbing.StubMapping
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.flatspec.AnyFlatSpec

class ExternalServiceSpec extends AnyFlatSpec with BeforeAndAfterEach with BeforeAndAfterAll {
  val wiremockS3 = new WireMockServer(8003)

  override def beforeAll(): Unit = {
    wiremockS3.start()
  }

  override def afterAll(): Unit = {
    wiremockS3.stop()
  }

  override def afterEach(): Unit = {
    wiremockS3.resetAll()
  }

  def mockS3GetResponse(): StubMapping = {
    wiremockS3.stubFor(WireMock.get(anyUrl()).willReturn(ok()))
  }
}
