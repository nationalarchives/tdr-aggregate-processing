package uk.gov.nationalarchives.aggregate.processing.modules.assetprocessing.metadata

import io.circe.syntax.EncoderOps
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import uk.gov.nationalarchives.aggregate.processing.{ExternalServiceSpec, MetadataHelper}
import uk.gov.nationalarchives.aggregate.processing.modules.Common.MetadataClassification

import java.util.UUID

class NetworkDriveMetadataHandlerSpec extends ExternalServiceSpec with MetadataHelper {
  private val expectedFilePath = "top-level/Retail/Shared Documents/file1.txt"
  private val matchId = "matchId"
  private val consignmentId = UUID.randomUUID()

  val networkDriveHandler: BaseMetadataHandler = NetworkDriveMetadataHandler.metadataHandler

  "normaliseValues" should "should not normalise any properties" in {
    val networkDrivePathJson = "top-level/folder/file1.txt".asJson
    val networkDriveLastModifiedJson = "1616162994000".asJson
    val someOtherJson = "some other json value".asJson

    networkDriveHandler.normaliseValues("file_path", networkDrivePathJson) shouldBe networkDrivePathJson
    networkDriveHandler.normaliseValues("date_last_modified", networkDriveLastModifiedJson) shouldBe networkDriveLastModifiedJson
    networkDriveHandler.normaliseValues("some_other_property", someOtherJson) shouldBe someOtherJson
  }

  "convertToBaseMetadata" should "convert valid network drive json with no default properties to base metadata json" in {
    val rawNetworkDriveJson = convertStringToJson(networkDriveJsonString(matchId, defaultFileSize, consignmentId, None, None))
    val expectedJson = convertStringToJson(validBaseMetadataJsonString(matchId, consignmentId, expectedFilePath))

    networkDriveHandler.convertToBaseMetadata(rawNetworkDriveJson) shouldBe expectedJson
  }

  "convertToBaseMetadata" should "throw an exception for invalid json" in {
    val exception = intercept[NoSuchElementException] {
      networkDriveHandler.convertToBaseMetadata("""some value}""".asJson)
    }
    exception.getMessage shouldBe "None.get"
  }

  "toClientSideMetadataInput" should "convert valid base metadata json to ClientSideMetadataInput" in {
    val input = networkDriveHandler.toClientSideMetadataInput(convertStringToJson(validBaseMetadataJsonString(matchId, consignmentId, expectedFilePath)))
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
    val input = networkDriveHandler.toClientSideMetadataInput(convertStringToJson("""{"someProperty": "some value"}"""))
    input.isLeft shouldBe true
    input.left.map(err => err.getMessage() shouldBe "DecodingFailure at .file_path: Missing required field")
  }

  "toMetadataProperties" should "return specified properties if exist in json" in {
    val properties = Seq("file_size", "file_name", "somePropertyNotInJson")
    val selectedMetadata = networkDriveHandler.toMetadataProperties(convertStringToJson(validBaseMetadataJsonString(matchId, consignmentId, expectedFilePath)), properties)
    selectedMetadata.size shouldBe 2
    selectedMetadata.contains(MetadataProperty("file_size", "12")) shouldBe true
    selectedMetadata.contains(MetadataProperty("file_name", "file1.txt")) shouldBe true
  }

  "classifyBaseMetadata" should "classify given metadata properties correctly" in {
    val sourceJson = convertStringToJson(validBaseMetadataWithSuppliedAndCustom(matchId, consignmentId, expectedFilePath))
    val classifiedMetadata = networkDriveHandler.classifyMetadata(sourceJson)
    classifiedMetadata(MetadataClassification.Custom) shouldEqual expectedCustomMetadata
    classifiedMetadata(MetadataClassification.Supplied) shouldEqual expectedSuppliedMetadata
    classifiedMetadata(MetadataClassification.System) shouldEqual expectedSystemMetadata(expectedFilePath)
  }
}
