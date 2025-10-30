package uk.gov.nationalarchives.aggregate.processing.utilities

import com.github.tototoshi.csv.CSVReader
import graphql.codegen.types.ClientSideMetadataInput
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.aggregate.processing.modules.AssetProcessing.{AssetProcessingResult, MetadataProperty}

class DraftMetadataCSVWriterSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  "createMetadataCSV" should "create a CSV file with the correct rows when systemMetadata and suppliedMetadata are provided" in {
    val writer = new DraftMetadataCSVWriter
    val assetProcessingResults = AssetProcessingResult(
      matchId = Some("1"),
      processingErrors = false,
      clientSideMetadataInput = Some(ClientSideMetadataInput(originalPath = "path", checksum = "checksum", lastModified = 1L, fileSize = 123L, matchId = "matchID")),
      systemMetadata = List(
        MetadataProperty("FileRef", "path/file1.txt"),
        MetadataProperty("FileLeafRef", "file1.txt"),
        MetadataProperty("Modified", "2022-03-15T10:20:30Z"),
        MetadataProperty("Length", "filesize")
      ),
      suppliedMetadata = List(
        MetadataProperty("description", "some kind of desc"),
        MetadataProperty("is filename closed", "true"),
        MetadataProperty("is description closed", "false"),
        MetadataProperty("copyright", "legal copyright")
      )
    )
    val actual = writer.createMetadataCSV(List(assetProcessingResults))
    val csvContent: List[List[String]] = CSVReader.open(actual).all()
    checkHeaders(csvContent)
    csvContent(1) shouldBe List(
      "path/file1.txt",
      "file1.txt",
      "2022-03-15",
      "",
      "some kind of desc",
      "",
      "Open",
      "",
      "",
      "",
      "",
      "Yes",
      "",
      "No",
      "",
      "English",
      "",
      "legal copyright",
      "",
      ""
    )
  }

  "convertValue" should "format date to yyyy-MM-dd" in {
    val writer = new DraftMetadataCSVWriter
    val assetProcessingResults = AssetProcessingResult(
      matchId = Some("1"),
      processingErrors = false,
      clientSideMetadataInput = Some(ClientSideMetadataInput(originalPath = "path", checksum = "checksum", lastModified = 1L, fileSize = 123L, matchId = "matchID")),
      systemMetadata = List(MetadataProperty(propertyName = "Modified", propertyValue = "2022-03-15T10:20:30Z")),
      suppliedMetadata = Nil
    )
    val actual = writer.createMetadataCSV(List(assetProcessingResults))
    val csvContent: List[List[String]] = CSVReader.open(actual).all()
    checkHeaders(csvContent)
    csvContent(1) shouldBe List("", "", "2022-03-15", "", "", "", "Open", "", "", "", "", "No", "", "No", "", "English", "", "Crown copyright", "", "")
  }

  it should "convert boolean true -> Yes and false -> No" in {
    val writer = new DraftMetadataCSVWriter
    val assetProcessingResults = AssetProcessingResult(
      matchId = Some("1"),
      processingErrors = false,
      clientSideMetadataInput = Some(ClientSideMetadataInput(originalPath = "path", checksum = "checksum", lastModified = 1L, fileSize = 123L, matchId = "matchID")),
      systemMetadata = Nil,
      suppliedMetadata = MetadataProperty(propertyName = "is filename closed", propertyValue = "true") ::
        MetadataProperty(propertyName = "is description closed", propertyValue = "false") :: Nil
    )
    val actual = writer.createMetadataCSV(List(assetProcessingResults))
    val csvContent: List[List[String]] = CSVReader.open(actual).all()
    checkHeaders(csvContent)
    csvContent(1) shouldBe List("", "", "", "", "", "", "Open", "", "", "", "", "Yes", "", "No", "", "English", "", "Crown copyright", "", "")
  }

  private def checkHeaders(csvContent: List[List[String]]) = {
    csvContent.head.size shouldBe 20
    csvContent.head should contain allOf (
      "filepath",
      "filename",
      "date last modified",
      "date of the record",
      "description",
      "former reference",
      "closure status",
      "closure start date",
      "closure period",
      "foi exemption code",
      "foi schedule date",
      "is filename closed",
      "alternate filename",
      "is description closed",
      "alternate description",
      "language",
      "translated filename",
      "copyright",
      "related material",
      "restrictions on use"
    )
  }
}
