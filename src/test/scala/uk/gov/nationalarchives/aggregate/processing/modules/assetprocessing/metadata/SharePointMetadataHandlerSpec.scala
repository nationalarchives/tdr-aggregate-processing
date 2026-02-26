package uk.gov.nationalarchives.aggregate.processing.modules.assetprocessing.metadata

import io.circe.syntax.EncoderOps
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import uk.gov.nationalarchives.aggregate.processing.{ExternalServiceSpec, MetadataHelper}
import uk.gov.nationalarchives.aggregate.processing.modules.Common.MetadataClassification

import java.util.UUID

class SharePointMetadataHandlerSpec extends ExternalServiceSpec with MetadataHelper {
  private val expectedFilePath = "sites/Retail/Shared Documents/file1.txt"
  private val matchId = "matchId"
  private val consignmentId = UUID.randomUUID()

  val handler: BaseMetadataHandler = SharePointMetadataHandler.metadataHandler

  "normaliseValues" should "normalise only specified property values" in {
    val dateTimeJson = "2025-07-03T09:19:47Z".asJson
    val filePathJson = "/sites/Retail/Shared Documents/file1.txt".asJson
    val someOtherJson = "some other json value".asJson

    handler.normaliseValues("closure_start_date", dateTimeJson) shouldBe "1751534387000".asJson
    handler.normaliseValues("date_last_modified", dateTimeJson) shouldBe "1751534387000".asJson
    handler.normaliseValues("end_date", dateTimeJson) shouldBe "1751534387000".asJson
    handler.normaliseValues("file_path", filePathJson) shouldBe expectedFilePath.asJson
    handler.normaliseValues("foi_schedule_date", dateTimeJson) shouldBe "1751534387000".asJson
    handler.normaliseValues("some_other_property", someOtherJson) shouldBe someOtherJson
  }

  "convertToBaseMetadata" should "convert valid SharePoint json to base metadata json" in {
    val rawSharePointJson = convertStringToJson(sharePointMetadataJsonString(matchId, defaultFileSize, consignmentId, None, None))
    val expectedJson = convertStringToJson(validBaseMetadataJsonString(matchId, consignmentId, expectedFilePath))

    handler.convertToBaseMetadata(rawSharePointJson) shouldBe expectedJson
  }

  "convertToBaseMetadata" should "convert valid SharePoint json to base metadata json when null date property present" in {
    val nullDateProperty = """"date_x0020_of_x0020_the_x0020_record": null"""
    val rawSharePointJson = convertStringToJson(sharePointMetadataJsonString(matchId, defaultFileSize, consignmentId, Some(nullDateProperty), None))
    val expectedJson = convertStringToJson(validBaseMetadataJsonString(matchId, consignmentId, expectedFilePath))

    handler.convertToBaseMetadata(rawSharePointJson) shouldBe expectedJson
  }

  "convertToBaseMetadata" should "throw an exception for invalid json" in {
    val exception = intercept[NoSuchElementException] {
      handler.convertToBaseMetadata("""some value}""".asJson)
    }
    exception.getMessage shouldBe "None.get"
  }

  "toClientSideMetadataInput" should "convert valid base metadata json to ClientSideMetadataInput" in {
    val input = handler.toClientSideMetadataInput(convertStringToJson(validBaseMetadataJsonString(matchId, consignmentId, expectedFilePath)))
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
    val selectedMetadata = handler.toMetadataProperties(convertStringToJson(validBaseMetadataJsonString(matchId, consignmentId, expectedFilePath)), properties)
    selectedMetadata.size shouldBe 2
    selectedMetadata.contains(MetadataProperty("file_size", "12")) shouldBe true
    selectedMetadata.contains(MetadataProperty("file_name", "file1.txt")) shouldBe true
  }

  "classifyBaseMetadata" should "classify given metadata properties correctly" in {
    val sourceJson = convertStringToJson(validBaseMetadataWithSuppliedAndCustom(matchId, consignmentId, expectedFilePath))
    val classifiedMetadata = handler.classifyBaseMetadata(sourceJson)
    classifiedMetadata(MetadataClassification.Custom) shouldEqual expectedCustomMetadata
    classifiedMetadata(MetadataClassification.Supplied) shouldEqual expectedSuppliedMetadata
    classifiedMetadata(MetadataClassification.System) shouldEqual expectedSystemMetadata(expectedFilePath)
  }
}
