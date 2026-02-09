package uk.gov.nationalarchives.aggregate.processing.modules.assetprocessing.metadata

import io.circe.syntax.EncoderOps
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import uk.gov.nationalarchives.aggregate.processing.ExternalServiceSpec
import uk.gov.nationalarchives.aggregate.processing.modules.Common.MetadataClassification

class DroidMetadataHandlerSpec extends ExternalServiceSpec {
  private val expectedFilePath = "Content/folder/file1.txt"

  val droidHandler: BaseMetadataHandler = DroidMetadataHandler.metadataHandler

  "normaliseValues" should "normalise only specified property values" in {
    val droidFilePathJson = """Z:\year_batch\batch_number\series\Content\folder\file1.txt""".asJson
    val droidDateLastModifiedJson = "2025-07-03T09:19:47".asJson
    val someOtherJson = "some other json value".asJson

    droidHandler.normaliseValues("file_path", droidFilePathJson) shouldBe expectedFilePath.asJson
    droidHandler.normaliseValues("date_last_modified", droidDateLastModifiedJson) shouldBe "1751534387000".asJson
    droidHandler.normaliseValues("some_other_property", someOtherJson) shouldBe someOtherJson
  }

  "convertToBaseMetadata" should "convert valid Droid json with no default properties to base metadata json" in {
    val rawDroidJsonString = """{
                               | "SIZE": "12",
                               | "LAST_MODIFIED": "2025-07-03T09:19:47",
                               | "NAME": "file1.txt",
                               | "FILE_PATH": "Z:\\year_batch\\batch_number\\series\\Content\\folder\\file1.txt",
                               | "SHA256_HASH": "1b47903dfdf5f21abeb7b304efb8e801656bff31225f522406f45c21a68eddf2",
                               | "matchId": "matchId",
                               | "transferId": "consignmentId"
    }""".stripMargin

    val rawSharePointJson = convertStringToJson(rawDroidJsonString)
    val expectedJson = convertStringToJson(validBaseMetadataJsonString(expectedFilePath))

    droidHandler.convertToBaseMetadata(rawSharePointJson) shouldBe expectedJson
  }

  "convertToBaseMetadata" should "throw an exception for invalid json" in {
    val exception = intercept[NoSuchElementException] {
      droidHandler.convertToBaseMetadata("""some value}""".asJson)
    }
    exception.getMessage shouldBe "None.get"
  }

  "toClientSideMetadataInput" should "convert valid base metadata json to ClientSideMetadataInput" in {
    val input = droidHandler.toClientSideMetadataInput(convertStringToJson(validBaseMetadataJsonString(expectedFilePath)))
    input.isLeft shouldBe false
    input.map(i => {
      i.matchId shouldBe "matchId"
      i.checksum shouldBe "1b47903dfdf5f21abeb7b304efb8e801656bff31225f522406f45c21a68eddf2"
      i.fileSize shouldBe 12L
      i.lastModified shouldBe 1751534387000L
      i.originalPath shouldBe "Content/folder/file1.txt"
    })
  }

  "toClientSideMetadataInput" should "return an error when converting invalid base metadata json" in {
    val input = droidHandler.toClientSideMetadataInput(convertStringToJson("""{"someProperty": "some value"}"""))
    input.isLeft shouldBe true
    input.left.map(err => err.getMessage() shouldBe "DecodingFailure at .file_path: Missing required field")
  }

  "toMetadataProperties" should "return specified properties if exist in json" in {
    val properties = Seq("file_size", "file_name", "somePropertyNotInJson")
    val selectedMetadata = droidHandler.toMetadataProperties(convertStringToJson(validBaseMetadataJsonString(expectedFilePath)), properties)
    selectedMetadata.size shouldBe 2
    selectedMetadata.contains(MetadataProperty("file_size", "12")) shouldBe true
    selectedMetadata.contains(MetadataProperty("file_name", "file1.txt")) shouldBe true
  }

  "classifyBaseMetadata" should "classify given metadata properties correctly" in {
    val sourceJson = convertStringToJson(baseMetadataWithSuppliedAndCustom())
    val classifiedMetadata = droidHandler.classifyBaseMetadata(sourceJson)
    classifiedMetadata(MetadataClassification.Custom) shouldEqual expectedCustomMetadata
    classifiedMetadata(MetadataClassification.Supplied) shouldEqual expectedSuppliedMetadata
    classifiedMetadata(MetadataClassification.System) shouldEqual expectedSystemMetadata
  }
}
