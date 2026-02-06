package uk.gov.nationalarchives.aggregate.processing

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.stubbing.StubMapping
import graphql.codegen.GetConsignment.getConsignment
import io.circe.{Json, Printer, parser}
import io.circe.generic.auto._
import io.circe.syntax._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import uk.gov.nationalarchives.aggregate.processing.modules.assetprocessing.metadata.MetadataProperty

import java.util.UUID
import scala.jdk.CollectionConverters.MapHasAsJava

class ExternalServiceSpec extends AnyFlatSpec with BeforeAndAfterEach with BeforeAndAfterAll with ScalaFutures {
  override implicit def patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(5, Seconds)), interval = scaled(Span(100, Millis)))

  val wiremockGraphqlServer = new WireMockServer(9001)
  val wiremockAuthServer = new WireMockServer(9002)
  val wiremockSfnServer = new WireMockServer(9003)
  val wiremockSsmServer = new WireMockServer(9004)
  val wiremockS3 = new WireMockServer(8003)

  val authPath = "/auth/realms/tdr/protocol/openid-connect/token"
  val graphQlPath = "/graphql"
  def graphQlUrl: String = wiremockGraphqlServer.url(graphQlPath)

  private def defaultMetadataJsonString(matchId: String, consignmentId: String) = s"""{
      "Length": "12",
      "Modified": "2025-07-03T09:19:47Z",
      "FileLeafRef": "file1.txt",
      "FileRef": "/sites/Retail/Shared Documents/file1.txt",
      "sha256ClientSideChecksum": "1b47903dfdf5f21abeb7b304efb8e801656bff31225f522406f45c21a68eddf2",
      "matchId": "$matchId",
      "transferId": "$consignmentId"
    }""".stripMargin

  private def defaultAndSuppliedMetadataJsonString(matchId: String, consignmentId: String) = s"""{
      "Length": "12",
      "Modified": "2025-07-03T09:19:47Z",
      "FileLeafRef": "file1.txt",
      "FileRef": "/sites/Retail/Shared Documents/file1.txt",
      "sha256ClientSideChecksum": "1b47903dfdf5f21abeb7b304efb8e801656bff31225f522406f45c21a68eddf2",
      "matchId": "$matchId",
      "transferId": "$consignmentId",
      "description": "some kind of description",
      "copyright": "legal copyright",
      "custom": "random value"
    }""".stripMargin

  def validBaseMetadataJsonString(filePath: String): String =
    s"""{
       | "file_size": "12",
       | "transferId": "consignmentId",
       | "file_path": "$filePath",
       | "matchId": "matchId",
       | "date_last_modified": "1751534387000",
       | "file_name": "file1.txt",
       | "client_side_checksum": "1b47903dfdf5f21abeb7b304efb8e801656bff31225f522406f45c21a68eddf2"
       |}""".stripMargin

  def baseMetadataWithSuppliedAndCustom(): String = {
    val matchId = UUID.randomUUID()
    val consignmentId = UUID.randomUUID()
    s"""{
      "file_size": "12",
      "date_last_modified": "2025-07-03T09:19:47Z",
      "file_name": "file1.txt",
      "file_path": "sites/Retail/Shared Documents/file1.txt",
      "client_side_checksum": "1b47903dfdf5f21abeb7b304efb8e801656bff31225f522406f45c21a68eddf2",
      "matchId": "$matchId",
      "transferId": "$consignmentId",
      "description": "some kind of description",
      "custom": "custom metadata value",
      "closure_type": "open"
    }""".stripMargin
  }

  val expectedSystemMetadata: List[MetadataProperty] = List(
    MetadataProperty("file_path", "sites/Retail/Shared Documents/file1.txt"),
    MetadataProperty("file_name", "file1.txt"),
    MetadataProperty("date_last_modified", "2025-07-03T09:19:47Z"),
    MetadataProperty("file_size", "12"),
    MetadataProperty("client_side_checksum", "1b47903dfdf5f21abeb7b304efb8e801656bff31225f522406f45c21a68eddf2")
  )

  val expectedSuppliedMetadata: List[MetadataProperty] = List(MetadataProperty("description", "some kind of description"), MetadataProperty("closure status", "open"))

  val expectedCustomMetadata: List[MetadataProperty] = List(MetadataProperty("custom", "custom metadata value"))

  def convertStringToJson(jsonString: String): Json = {
    parser.parse(jsonString).fold(err => throw new RuntimeException(err.getMessage()), j => j)
  }

  def authOkJson(): StubMapping = wiremockAuthServer.stubFor(
    post(urlEqualTo(authPath))
      .willReturn(okJson("""{"access_token": "abcde"}"""))
  )

  def setupSsmServer(): Unit = {
    wiremockSsmServer
      .stubFor(
        post(urlEqualTo("/"))
          .willReturn(okJson("{\"Parameter\":{\"Name\":\"string\",\"Value\":\"string\"}}"))
      )
  }

  override def beforeAll(): Unit = {
    wiremockAuthServer.start()
    wiremockGraphqlServer.start()
    wiremockS3.start()
    wiremockSfnServer.start()
    wiremockSsmServer.start()
  }

  override def afterAll(): Unit = {
    wiremockAuthServer.stop()
    wiremockGraphqlServer.stop()
    wiremockS3.stop()
    wiremockSfnServer.stop()
    wiremockSsmServer.stop()
  }

  override def beforeEach(): Unit = {
    setupSsmServer()
  }

  override def afterEach(): Unit = {
    wiremockAuthServer.resetAll()
    wiremockGraphqlServer.resetAll()
    wiremockS3.resetAll()
    wiremockSfnServer.resetAll()
    wiremockSsmServer.resetAll()
  }

  def mockGraphQlAddFilesAndMetadataResponse: StubMapping = wiremockGraphqlServer.stubFor(
    post(urlEqualTo(graphQlPath))
      .withRequestBody(containing("addFilesAndMetadata"))
      .willReturn(ok("""{"data": {"addFilesAndMetadata": [{"fileId": "8b5bae20-5f12-11eb-ae93-0242ac130002", "matchId": "1"}]}}""".stripMargin))
  )

  def mockGraphQlUpdateConsignmentStatusResponse: StubMapping = wiremockGraphqlServer.stubFor(
    post(urlEqualTo(graphQlPath))
      .withRequestBody(containing("updateConsignmentStatus"))
      .willReturn(ok("""{"data": {"updateConsignmentStatus": 1}}""".stripMargin))
  )

  def mockGraphQlGetConsignmentResponse: StubMapping = {
    val data = Some(
      getConsignment.Data(
        Some(
          getConsignment.GetConsignment(
            UUID.randomUUID(),
            None,
            None,
            "ConsignmentRef",
            None,
            None,
            Some("TransferringBody"),
            Nil
          )
        )
      )
    )
    val dataString: String = data.asJson.printWith(Printer(dropNullValues = false, ""))
    wiremockGraphqlServer.stubFor(
      post(urlEqualTo(graphQlPath))
        .withRequestBody(containing("getConsignment"))
        .willReturn(ok(dataString))
    )
  }

  def mockGraphQlUpdateParentFolderResponse: StubMapping =
    wiremockGraphqlServer.stubFor(
      post(urlEqualTo("/graphql"))
        .withRequestBody(containing("updateParentFolder"))
        .willReturn(ok("""{"data":{"updateParentFolder": 1}}"""))
    )

  def mockGraphQlResponseError: StubMapping = wiremockGraphqlServer.stubFor(
    post(urlEqualTo(graphQlPath))
      .willReturn(aResponse().withStatus(500).withBody("internal server error"))
  )

  def mockS3GetObjectStream(key: String, consignmentId: String, matchId: String, suppliedMetadata: Boolean = false): StubMapping = {
    val bytes = if (suppliedMetadata) {
      defaultAndSuppliedMetadataJsonString(matchId, consignmentId).getBytes("UTF-8")
    } else {
      defaultMetadataJsonString(matchId, consignmentId).getBytes("UTF-8")
    }
    wiremockS3.stubFor(
      get(urlEqualTo(s"/$key"))
        .willReturn(aResponse().withStatus(200).withBody(bytes))
    )
  }

  def mockS3GetObjectTagging(key: String): StubMapping = {
    val response = <Tagging>
      <TagSet>
        <Tag>
          <Key>GuardDutyMalwareScanStatus</Key>
          <Value>NO_THREATS_FOUND</Value>
        </Tag>
      </TagSet>
    </Tagging>

    wiremockS3.stubFor(
      get(urlEqualTo(s"/$key?tagging"))
        .willReturn(okXml(response.toString()))
    )
  }

  def mockS3GetObjectStreamError(): StubMapping = {
    wiremockS3.stubFor(
      get(anyUrl())
        .willReturn(aResponse().withStatus(500).withBody("Internal server error"))
    )
  }

  def mockS3ListBucketResponse(userId: UUID, consignmentId: UUID, matchIds: List[String]): StubMapping = {
    val params = Map("list-type" -> equalTo("2"), "prefix" -> equalTo(s"$userId/sharepoint/$consignmentId/metadata")).asJava
    val response = <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
      {
      matchIds.map(matchId => <Contents>
          <Key>{userId}/sharepoint/{consignmentId}/metadata/{matchId}.metadata</Key>
          <LastModified>2009-10-12T17:50:30.000Z</LastModified>
          <ETag>"fba9dede5f27731c9771645a39863328"</ETag>
          <Size>1</Size>
        </Contents>)
    }
    </ListBucketResult>
    wiremockS3.stubFor(
      get(anyUrl())
        .withQueryParams(params)
        .willReturn(okXml(response.toString))
    )
  }

  def mockS3ListBucketResponseEmpty(userId: UUID, consignmentId: UUID): StubMapping = {
    val params = Map("list-type" -> equalTo("2"), "prefix" -> equalTo(s"$userId/sharepoint/$consignmentId/metadata")).asJava
    val response = <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    </ListBucketResult>
    wiremockS3.stubFor(
      get(anyUrl())
        .withQueryParams(params)
        .willReturn(okXml(response.toString))
    )
  }

  def mockS3ListBucketResponseError(userId: UUID, consignmentId: UUID): StubMapping = {
    val params = Map("list-type" -> equalTo("2"), "prefix" -> equalTo(s"$userId/sharepoint/$consignmentId/metadata")).asJava
    wiremockS3.stubFor(
      get(anyUrl())
        .withQueryParams(params)
        .willReturn(aResponse().withStatus(500).withBody("Internal server error"))
    )
  }

  def mockSfnResponseOk(): StubMapping = {
    wiremockSfnServer.stubFor(
      post(anyUrl())
        .withRequestBody(containing("stateMachineArn"))
        .willReturn(aResponse().withStatus(200))
    )
  }
}
