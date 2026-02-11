package uk.gov.nationalarchives.aggregate.processing.modules.assetprocessing.metadata

import io.circe.syntax.EncoderOps
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import uk.gov.nationalarchives.aggregate.processing.{ExternalServiceSpec, MetadataHelper}
import uk.gov.nationalarchives.aggregate.processing.modules.Common.MetadataClassification

import java.util.UUID

class HardDriveMetadataHandlerSpec extends ExternalServiceSpec with MetadataHelper {
  private val expectedFilePath = "content/Retail/Shared Documents/file1.txt"
  private val matchId = "matchId"
  private val consignmentId = UUID.randomUUID()

  val hardDriveHandler: BaseMetadataHandler = HardDriveMetadataHandler.metadataHandler

  "normaliseValues" should "normalise only specified property values" in {
    val hardDriveFilePathJson = """Z:\series\content\Retail\Shared Documents\file1.txt""".asJson
    val hardDriveDateLastModifiedJson = "2025-07-03T09:19:47".asJson
    val someOtherJson = "some other json value".asJson

    hardDriveHandler.normaliseValues("closure_period", "0".asJson) shouldBe "".asJson
    hardDriveHandler.normaliseValues("closure_period", "12".asJson) shouldBe "12".asJson
    hardDriveHandler.normaliseValues("closure_type", "open_on_transfer".asJson) shouldBe "Open".asJson
    hardDriveHandler.normaliseValues("closure_type", "closed_on_transfer".asJson) shouldBe "Closed".asJson
    hardDriveHandler.normaliseValues("description_closed", "true".asJson) shouldBe "false".asJson
    hardDriveHandler.normaliseValues("description_closed", "false".asJson) shouldBe "true".asJson
    hardDriveHandler.normaliseValues("file_path", hardDriveFilePathJson) shouldBe expectedFilePath.asJson
    hardDriveHandler.normaliseValues("date_last_modified", hardDriveDateLastModifiedJson) shouldBe "1751534387000".asJson
    hardDriveHandler.normaliseValues("title_closed", "true".asJson) shouldBe "false".asJson
    hardDriveHandler.normaliseValues("title_closed", "false".asJson) shouldBe "true".asJson
    hardDriveHandler.normaliseValues("foi_exemption_code", "Open".asJson) shouldBe "".asJson
    hardDriveHandler.normaliseValues("foi_exemption_code", "30".asJson) shouldBe "30".asJson
    hardDriveHandler.normaliseValues("some_other_property", someOtherJson) shouldBe someOtherJson
  }

  "convertToBaseMetadata" should "convert valid hard drive json with no default properties to base metadata json" in {
    val rawSharePointJson = convertStringToJson(hardDriveMetadataJsonString(matchId, defaultFileSize, consignmentId, None, None))
    val expectedJson = convertStringToJson(validBaseMetadataJsonString(matchId, consignmentId, expectedFilePath))

    hardDriveHandler.convertToBaseMetadata(rawSharePointJson) shouldBe expectedJson
  }

  "convertToBaseMetadata" should "throw an exception for invalid json" in {
    val exception = intercept[NoSuchElementException] {
      hardDriveHandler.convertToBaseMetadata("""some value}""".asJson)
    }
    exception.getMessage shouldBe "None.get"
  }

  "toClientSideMetadataInput" should "convert valid base metadata json to ClientSideMetadataInput" in {
    val input = hardDriveHandler.toClientSideMetadataInput(convertStringToJson(validBaseMetadataJsonString(matchId, consignmentId, expectedFilePath)))
    input.isLeft shouldBe false
    input.map(i => {
      i.matchId shouldBe "matchId"
      i.checksum shouldBe "1b47903dfdf5f21abeb7b304efb8e801656bff31225f522406f45c21a68eddf2"
      i.fileSize shouldBe 12L
      i.lastModified shouldBe 1751534387000L
      i.originalPath shouldBe expectedFilePath
    })
  }

  "toClientSideMetadataInput" should "return an error when converting invalid base metadata json" in {
    val input = hardDriveHandler.toClientSideMetadataInput(convertStringToJson("""{"someProperty": "some value"}"""))
    input.isLeft shouldBe true
    input.left.map(err => err.getMessage() shouldBe "DecodingFailure at .file_path: Missing required field")
  }

  "toMetadataProperties" should "return specified properties if exist in json" in {
    val properties = Seq("file_size", "file_name", "somePropertyNotInJson")
    val selectedMetadata = hardDriveHandler.toMetadataProperties(convertStringToJson(validBaseMetadataJsonString(matchId, consignmentId, expectedFilePath)), properties)
    selectedMetadata.size shouldBe 2
    selectedMetadata.contains(MetadataProperty("file_size", "12")) shouldBe true
    selectedMetadata.contains(MetadataProperty("file_name", "file1.txt")) shouldBe true
  }

  "classifyBaseMetadata" should "classify given metadata properties correctly" in {
    val sourceJson = convertStringToJson(validBaseMetadataWithSuppliedAndCustom(matchId, consignmentId, expectedFilePath))
    val classifiedMetadata = hardDriveHandler.classifyMetadata(sourceJson)
    classifiedMetadata(MetadataClassification.Custom) shouldEqual expectedCustomMetadata
    classifiedMetadata(MetadataClassification.Supplied) shouldEqual expectedSuppliedMetadata
    classifiedMetadata(MetadataClassification.System) shouldEqual expectedSystemMetadata(expectedFilePath)
  }
}
