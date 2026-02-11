package uk.gov.nationalarchives.aggregate.processing.modules.assetprocessing.metadata

import io.circe.syntax.EncoderOps
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import uk.gov.nationalarchives.aggregate.processing.{ExternalServiceSpec, MetadataHelper}
import uk.gov.nationalarchives.aggregate.processing.modules.Common.MetadataClassification

import java.util.UUID

class DroidMetadataHandlerSpec extends ExternalServiceSpec with MetadataHelper {
  private val expectedFilePath = "Content/Retail/Shared Documents/file1.txt"
  private val matchId = "matchId"
  private val consignmentId = UUID.randomUUID()

  val droidHandler: BaseMetadataHandler = DroidMetadataHandler.metadataHandler

  "normaliseValues" should "normalise only specified property values" in {
    val droidFilePathJson = """Z:\year_batch\batch_number\series\Content\Retail\Shared Documents\file1.txt""".asJson
    val droidDateLastModifiedJson = "2025-07-03T09:19:47".asJson
    val someOtherJson = "some other json value".asJson

    droidHandler.normaliseValues("file_path", droidFilePathJson) shouldBe expectedFilePath.asJson
    droidHandler.normaliseValues("date_last_modified", droidDateLastModifiedJson) shouldBe "1751534387000".asJson
    droidHandler.normaliseValues("some_other_property", someOtherJson) shouldBe someOtherJson
  }

  "convertToBaseMetadata" should "convert valid Droid json with no default properties to base metadata json" in {
    val rawSharePointJson = convertStringToJson(droidMetadataJsonString(matchId, defaultFileSize, consignmentId, None, None))
    val expectedJson = convertStringToJson(validBaseMetadataJsonString(matchId, consignmentId, expectedFilePath))

    droidHandler.convertToBaseMetadata(rawSharePointJson) shouldBe expectedJson
  }

  "convertToBaseMetadata" should "throw an exception for invalid json" in {
    val exception = intercept[NoSuchElementException] {
      droidHandler.convertToBaseMetadata("""some value}""".asJson)
    }
    exception.getMessage shouldBe "None.get"
  }

  "toClientSideMetadataInput" should "convert valid base metadata json to ClientSideMetadataInput" in {
    val input = droidHandler.toClientSideMetadataInput(convertStringToJson(validBaseMetadataJsonString(matchId, consignmentId, expectedFilePath)))
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
    val input = droidHandler.toClientSideMetadataInput(convertStringToJson("""{"someProperty": "some value"}"""))
    input.isLeft shouldBe true
    input.left.map(err => err.getMessage() shouldBe "DecodingFailure at .file_path: Missing required field")
  }

  "toMetadataProperties" should "return specified properties if exist in json" in {
    val properties = Seq("file_size", "file_name", "somePropertyNotInJson")
    val selectedMetadata = droidHandler.toMetadataProperties(convertStringToJson(validBaseMetadataJsonString(matchId, consignmentId, expectedFilePath)), properties)
    selectedMetadata.size shouldBe 2
    selectedMetadata.contains(MetadataProperty("file_size", "12")) shouldBe true
    selectedMetadata.contains(MetadataProperty("file_name", "file1.txt")) shouldBe true
  }

  "classifyMetadata" should "classify given metadata properties correctly" in {
    val sourceJson = convertStringToJson(validBaseMetadataWithSuppliedAndCustom(matchId, consignmentId, expectedFilePath))
    val classifiedMetadata = droidHandler.classifyMetadata(sourceJson)
    classifiedMetadata(MetadataClassification.Custom) shouldEqual expectedCustomMetadata
    classifiedMetadata(MetadataClassification.Supplied) shouldEqual expectedSuppliedMetadata
    classifiedMetadata(MetadataClassification.System) shouldEqual expectedSystemMetadata(expectedFilePath)
  }
}
