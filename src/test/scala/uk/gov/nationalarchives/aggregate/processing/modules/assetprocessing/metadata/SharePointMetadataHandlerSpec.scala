package uk.gov.nationalarchives.aggregate.processing.modules.assetprocessing.metadata

import io.circe.syntax.EncoderOps
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import uk.gov.nationalarchives.aggregate.processing.ExternalServiceSpec
import uk.gov.nationalarchives.aggregate.processing.modules.Common.MetadataClassification

import java.util.UUID

class SharePointMetadataHandlerSpec extends ExternalServiceSpec {
  private val expectedFilePath = "sites/Retail/Shared Documents/file1.txt"

  val handler: BaseMetadataHandler = SharePointMetadataHandler.metadataHandler

  "normaliseValues" should "normalise only specified property values" in {
    val filePathJson = "/sites/Retail/Shared Documents/file1.txt".asJson
    val dateLastModifiedJson = "2025-07-03T09:19:47Z".asJson
    val someOtherJson = "some other json value".asJson

    handler.normaliseValues("file_path", filePathJson) shouldBe expectedFilePath.asJson
    handler.normaliseValues("date_last_modified", dateLastModifiedJson) shouldBe "1751534387000".asJson
    handler.normaliseValues("some_other_property", someOtherJson) shouldBe someOtherJson
  }

  "convertToBaseMetadata" should "convert valid SharePoint json to base metadata json" in {
    val rawSharePointJsonString = """{
      | "Length": "12",
      | "Modified": "2025-07-03T09:19:47Z",
      | "FileLeafRef": "file1.txt",
      | "FileRef": "/sites/Retail/Shared Documents/file1.txt",
      | "sha256ClientSideChecksum": "1b47903dfdf5f21abeb7b304efb8e801656bff31225f522406f45c21a68eddf2",
      | "matchId": "matchId",
      | "transferId": "consignmentId"
    }""".stripMargin

    val rawSharePointJson = convertStringToJson(rawSharePointJsonString)
    val expectedJson = convertStringToJson(validBaseMetadataJsonString(expectedFilePath))

    handler.convertToBaseMetadata(rawSharePointJson) shouldBe expectedJson
  }

  "convertToBaseMetadata" should "throw an exception for invalid json" in {
    val exception = intercept[NoSuchElementException] {
      handler.convertToBaseMetadata("""some value}""".asJson)
    }
    exception.getMessage shouldBe "None.get"
  }

  "toClientSideMetadataInput" should "convert valid base metadata json to ClientSideMetadataInput" in {
    val input = handler.toClientSideMetadataInput(convertStringToJson(validBaseMetadataJsonString(expectedFilePath)))
    input.isLeft shouldBe false
    input.map(i => {
      i.matchId shouldBe "matchId"
      i.checksum shouldBe "1b47903dfdf5f21abeb7b304efb8e801656bff31225f522406f45c21a68eddf2"
      i.fileSize shouldBe 12L
      i.lastModified shouldBe 1751534387000L
      i.originalPath shouldBe "sites/Retail/Shared Documents/file1.txt"
    })
  }

  "toClientSideMetadataInput" should "return an error when converting invalid base metadata json" in {
    val input = handler.toClientSideMetadataInput(convertStringToJson("""{"someProperty": "some value"}"""))
    input.isLeft shouldBe true
    input.left.map(err => err.getMessage() shouldBe "DecodingFailure at .file_path: Missing required field")
  }

  "toMetadataProperties" should "return specified properties if exist in json" in {
    val properties = Seq("file_size", "file_name", "somePropertyNotInJson")
    val selectedMetadata = handler.toMetadataProperties(convertStringToJson(validBaseMetadataJsonString(expectedFilePath)), properties)
    selectedMetadata.size shouldBe 2
    selectedMetadata.contains(MetadataProperty("file_size", "12")) shouldBe true
    selectedMetadata.contains(MetadataProperty("file_name", "file1.txt")) shouldBe true
  }

  "classifyBaseMetadata" should "classify given metadata properties correctly" in {
    val matchId = UUID.randomUUID()
    val consignmentId = UUID.randomUUID()
    val baseMetadataWithSuppliedAndCustom = s"""{
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

    val sourceJson = convertStringToJson(baseMetadataWithSuppliedAndCustom)
    val classifiedMetadata = handler.classifyBaseMetadata(sourceJson)
    classifiedMetadata(MetadataClassification.Custom) shouldEqual expectedCustomMetadata
    classifiedMetadata(MetadataClassification.Supplied) shouldEqual expectedSuppliedMetadata
    classifiedMetadata(MetadataClassification.System) shouldEqual expectedSystemMetadata
  }
}
