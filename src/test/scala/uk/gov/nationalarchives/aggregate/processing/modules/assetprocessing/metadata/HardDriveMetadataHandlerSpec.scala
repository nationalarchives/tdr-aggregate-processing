package uk.gov.nationalarchives.aggregate.processing.modules.assetprocessing.metadata

import io.circe.syntax.EncoderOps
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import uk.gov.nationalarchives.aggregate.processing.ExternalServiceSpec
import uk.gov.nationalarchives.aggregate.processing.modules.Common.MetadataClassification

class HardDriveMetadataHandlerSpec extends ExternalServiceSpec {
  private val expectedFilePath = "content/folder/file1.txt"

  val hardDriveHandler: BaseMetadataHandler = HardDriveMetadataHandler.metadataHandler

  "normaliseValues" should "normalise only specified property values" in {
    val hardDriveFilePathJson = """Z:\series\content\folder\file1.txt""".asJson
    val hardDriveDateLastModifiedJson = "2025-07-03T09:19:47".asJson
    val someOtherJson = "some other json value".asJson

    hardDriveHandler.normaliseValues("closure_period", "0".asJson) shouldBe "".asJson
    hardDriveHandler.normaliseValues("closure_period", "12".asJson) shouldBe "12".asJson
    hardDriveHandler.normaliseValues("closure_type", "open_on_transfer".asJson) shouldBe "open".asJson
    hardDriveHandler.normaliseValues("closure_type", "closed_on_transfer".asJson) shouldBe "closed".asJson
    hardDriveHandler.normaliseValues("description_closed", "true".asJson) shouldBe "false".asJson
    hardDriveHandler.normaliseValues("description_closed", "false".asJson) shouldBe "true".asJson
    hardDriveHandler.normaliseValues("file_path", hardDriveFilePathJson) shouldBe expectedFilePath.asJson
    hardDriveHandler.normaliseValues("date_last_modified", hardDriveDateLastModifiedJson) shouldBe "1751534387000".asJson
    hardDriveHandler.normaliseValues("title_closed", "true".asJson) shouldBe "false".asJson
    hardDriveHandler.normaliseValues("title_closed", "false".asJson) shouldBe "true".asJson
    hardDriveHandler.normaliseValues("some_other_property", someOtherJson) shouldBe someOtherJson
  }

  "convertToBaseMetadata" should "convert valid hard drive json with no default properties to base metadata json" in {
    val rawHardDriveJsonString = """{
                                   | "file_size": "12",
                                   | "date_last_modified": "2025-07-03T09:19:47",
                                   | "file_name": "file1.txt",
                                   | "file_path": "Z:\\year_batch\\batch_number\\series\\content\\folder\\file1.txt",
                                   | "checksum": "1b47903dfdf5f21abeb7b304efb8e801656bff31225f522406f45c21a68eddf2",
                                   | "matchId": "matchId",
                                   | "transferId": "consignmentId"
    }""".stripMargin

    val rawSharePointJson = convertStringToJson(rawHardDriveJsonString)
    val expectedJson = convertStringToJson(validBaseMetadataJsonString(expectedFilePath))

    hardDriveHandler.convertToBaseMetadata(rawSharePointJson) shouldBe expectedJson
  }

  "convertToBaseMetadata" should "throw an exception for invalid json" in {
    val exception = intercept[NoSuchElementException] {
      hardDriveHandler.convertToBaseMetadata("""some value}""".asJson)
    }
    exception.getMessage shouldBe "None.get"
  }

  "toClientSideMetadataInput" should "convert valid base metadata json to ClientSideMetadataInput" in {
    val input = hardDriveHandler.toClientSideMetadataInput(convertStringToJson(validBaseMetadataJsonString(expectedFilePath)))
    input.isLeft shouldBe false
    input.map(i => {
      i.matchId shouldBe "matchId"
      i.checksum shouldBe "1b47903dfdf5f21abeb7b304efb8e801656bff31225f522406f45c21a68eddf2"
      i.fileSize shouldBe 12L
      i.lastModified shouldBe 1751534387000L
      i.originalPath shouldBe "content/folder/file1.txt"
    })
  }

  "toClientSideMetadataInput" should "return an error when converting invalid base metadata json" in {
    val input = hardDriveHandler.toClientSideMetadataInput(convertStringToJson("""{"someProperty": "some value"}"""))
    input.isLeft shouldBe true
    input.left.map(err => err.getMessage() shouldBe "DecodingFailure at .file_path: Missing required field")
  }

  "toMetadataProperties" should "return specified properties if exist in json" in {
    val properties = Seq("file_size", "file_name", "somePropertyNotInJson")
    val selectedMetadata = hardDriveHandler.toMetadataProperties(convertStringToJson(validBaseMetadataJsonString(expectedFilePath)), properties)
    selectedMetadata.size shouldBe 2
    selectedMetadata.contains(MetadataProperty("file_size", "12")) shouldBe true
    selectedMetadata.contains(MetadataProperty("file_name", "file1.txt")) shouldBe true
  }

  "classifyMetadata" should "classify given metadata properties correctly" in {
    val sourceJson = convertStringToJson(baseMetadataWithSuppliedAndCustom())
    val classifiedMetadata = hardDriveHandler.classifyMetadata(sourceJson)
    classifiedMetadata(MetadataClassification.Custom) shouldEqual expectedCustomMetadata
    classifiedMetadata(MetadataClassification.Supplied) shouldEqual expectedSuppliedMetadata
    classifiedMetadata(MetadataClassification.System) shouldEqual expectedSystemMetadata
  }
}
