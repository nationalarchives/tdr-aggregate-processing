package uk.gov.nationalarchives.aggregate.processing.modules.assetprocessing.metadata

import io.circe.JsonObject
import io.circe.syntax.EncoderOps
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import uk.gov.nationalarchives.aggregate.processing.{ExternalServiceSpec, MetadataHelper}
import uk.gov.nationalarchives.aggregate.processing.modules.Common.MetadataClassification

import java.util.UUID

class SharePointMetadataHandlerSpec extends ExternalServiceSpec with MetadataHelper {
  private val siteDisplayName = "Site Display Name"
  private val libraryDisplayName = "Library Display Name"
  private val expectedFilePath = "Retail/Shared Documents/aFolder/file1.txt"
  private val matchId = "matchId"
  private val consignmentId = UUID.randomUUID()

  val handler: BaseMetadataHandler = SharePointMetadataHandler.metadataHandler

  "normaliseValues" should "normalise only specified property values" in {
    val dateTimeJson = "2025-07-03T09:19:47Z".asJson
    val filePathJson = "/sites/Retail/Shared Documents/aFolder/file1.txt".asJson
    val someOtherJson = "some other json value".asJson
    val allJsonMetadata = JsonObject()

    val expectedSiteNameIgnoredPath = "Shared Documents/aFolder/file1.txt"

    handler.normaliseValues(NormaliseValueInput("closure_start_date", dateTimeJson, allJsonMetadata)) shouldBe "1751534387000".asJson
    handler.normaliseValues(NormaliseValueInput("date_last_modified", dateTimeJson, allJsonMetadata)) shouldBe "1751534387000".asJson
    handler.normaliseValues(NormaliseValueInput("end_date", dateTimeJson, allJsonMetadata)) shouldBe "1751534387000".asJson
    handler.normaliseValues(NormaliseValueInput("file_path", filePathJson, allJsonMetadata)) shouldBe expectedFilePath.asJson
    handler.normaliseValues(
      NormaliseValueInput("file_path", filePathJson, allJsonMetadata, ignoreSiteName = true)
    ) shouldBe expectedSiteNameIgnoredPath.asJson
    handler.normaliseValues(NormaliseValueInput("foi_exemption_asserted", dateTimeJson, allJsonMetadata)) shouldBe "1751534387000".asJson
    handler.normaliseValues(NormaliseValueInput("closure_period", 20.asJson, allJsonMetadata)) shouldBe "20".asJson
    handler.normaliseValues(NormaliseValueInput("some_other_property", someOtherJson, allJsonMetadata)) shouldBe someOtherJson
  }

  "normaliseValues" should "return normalised file path based on whether display names are present" in {
    val filePathJson = "/sites/Retail/Shared Documents/aFolder/file1.txt".asJson
    val allJsonMetadata = JsonObject()
      .add("SiteName", siteDisplayName.asJson)
      .add("LibraryName", libraryDisplayName.asJson)

    handler.normaliseValues(NormaliseValueInput("file_path", filePathJson, allJsonMetadata)) shouldBe
      "Site Display Name/Library Display Name/aFolder/file1.txt".asJson

    val allJsonMetadataLibraryNameOnly = JsonObject()
      .add("LibraryName", libraryDisplayName.asJson)

    handler.normaliseValues(NormaliseValueInput("file_path", filePathJson, allJsonMetadataLibraryNameOnly)) shouldBe
      "Retail/Library Display Name/aFolder/file1.txt".asJson

    val allJsonMetadataSiteNameOnly = JsonObject()
      .add("SiteName", siteDisplayName.asJson)

    handler.normaliseValues(NormaliseValueInput("file_path", filePathJson, allJsonMetadataSiteNameOnly)) shouldBe
      "Site Display Name/Shared Documents/aFolder/file1.txt".asJson
  }

  "convertToBaseMetadata" should "convert valid SharePoint json to base metadata json" in {
    val rawSharePointJson = convertStringToJson(sharePointMetadataJsonString(matchId, defaultFileSize, consignmentId, None, None))
    val expectedJson = convertStringToJson(validBaseMetadataJsonString(matchId, consignmentId, expectedFilePath))

    handler.convertToBaseMetadata(rawSharePointJson, ignoreSiteName = false) shouldBe expectedJson
  }

  "convertToBaseMetadata" should "convert valid SharePoint json to base metadata json when null date property present" in {
    val nullDateProperty = """"date_x0020_of_x0020_the_x0020_record": null"""
    val rawSharePointJson = convertStringToJson(sharePointMetadataJsonString(matchId, defaultFileSize, consignmentId, Some(nullDateProperty), None))
    val expectedJson = convertStringToJson(validBaseMetadataJsonString(matchId, consignmentId, expectedFilePath))

    handler.convertToBaseMetadata(rawSharePointJson, ignoreSiteName = false) shouldBe expectedJson
  }

  "convertToBaseMetadata" should "throw an exception for invalid json" in {
    val exception = intercept[NoSuchElementException] {
      handler.convertToBaseMetadata("""some value}""".asJson, ignoreSiteName = false)
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
      i.originalPath shouldBe "Retail/Shared Documents/aFolder/file1.txt"
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
