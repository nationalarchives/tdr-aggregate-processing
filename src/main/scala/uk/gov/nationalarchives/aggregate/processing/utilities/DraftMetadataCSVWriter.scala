package uk.gov.nationalarchives.aggregate.processing.utilities

import com.github.tototoshi.csv.CSVWriter
import uk.gov.nationalarchives.aggregate.processing.modules.assetprocessing.AssetProcessing
import uk.gov.nationalarchives.tdr.schemautils.ConfigUtils

import java.io.File
import java.nio.file.Files
import java.text.SimpleDateFormat

class DraftMetadataCSVWriter {

  def createMetadataCSV(assetProcessingResults: List[AssetProcessing.AssetProcessingResult]): File = {
    val metadataConfiguration = ConfigUtils.loadConfiguration
    val downloadDisplayProperties = metadataConfiguration
      .downloadFileDisplayProperties("MetadataDownloadTemplate")
      .sortBy(_.columnIndex)

    val keyToTdrFileHeader = metadataConfiguration.propertyToOutputMapper("tdrFileHeader")
    val tdrHeaderToKey = metadataConfiguration.inputToPropertyMapper("tdrFileHeader")
    val sharepointHeaderToKey = metadataConfiguration.inputToPropertyMapper("sharePointTag")

    // --- Determine headers ---
    val dynamicHeaders: List[String] = downloadDisplayProperties.map(dp => keyToTdrFileHeader(dp.key))
    val colHeaders: List[String] = dynamicHeaders

    // Helper to convert a sequence of (key, value) to header -> convertedValue using provided mappers
    def toHeaderMap(entries: Seq[(String, String)])(keyToHeader: String => String, keyToType: String => String): Map[String, String] =
      entries.map { case (k, v) =>
        keyToHeader(k) -> convertValue(keyToType(k), v)
      }.toMap

    // --- Build rows ---
    val rows: List[List[String]] = assetProcessingResults.map { result =>
      // defaults: properties are keyed by property key
      val defaultEntries: Seq[(String, String)] = metadataConfiguration.getPropertiesWithDefaultValue.toList
      val propertiesDefaultValues: Map[String, String] =
        toHeaderMap(defaultEntries)(keyToTdrFileHeader, metadataConfiguration.getPropertyType)

      // supplied metadata: entries are already using TDR headers, need to lookup property type via tdrHeaderToKey
      val suppliedEntries: Seq[(String, String)] = result.suppliedMetadata.map(sm => (sm.propertyName, sm.propertyValue))
      val suppliedMetadataMap: Map[String, String] =
        toHeaderMap(suppliedEntries)(identity, header => metadataConfiguration.getPropertyType(tdrHeaderToKey(header)))

      // system metadata: entries use SharePoint headers, map to TDR header first
      val systemEntries: Seq[(String, String)] = result.systemMetadata.map(sm => (sm.propertyName, sm.propertyValue))
      val systemMetadataMap: Map[String, String] =
        toHeaderMap(systemEntries)(
          spHeader => keyToTdrFileHeader(sharepointHeaderToKey(spHeader)),
          spHeader => metadataConfiguration.getPropertyType(sharepointHeaderToKey(spHeader))
        )

      // For each header pick the first non-empty value from supplied, default, system (in that precedence)
      dynamicHeaders.map { header =>
        val supplied = suppliedMetadataMap.getOrElse(header, "")
        val default = propertiesDefaultValues.getOrElse(header, "")
        val system = systemMetadataMap.getOrElse(header, "")
        List(supplied, default, system).find(_.nonEmpty).getOrElse("")
      }
    }

    // --- Write CSV ---
    val csvFile = Files.createTempFile("draft-metadata", ".csv").toFile
    val writer = CSVWriter.open(csvFile)

    try {
      writer.writeRow(colHeaders)
      writer.writeAll(rows)
    } finally {
      writer.close()
    }
    csvFile
  }

  private def convertValue(propertyType: String, fileMetadataValue: String): String = {
    propertyType match {
      case "date"    => convertToLocalDateOrString(fileMetadataValue)
      case "boolean" => if (fileMetadataValue == "true") "Yes" else "No"
      case _         => fileMetadataValue
    }
  }

  private def convertToLocalDateOrString(timestampString: String): String = {
    if (timestampString.isEmpty) {
      timestampString
    } else {
      val ts = timestampString.toLong
      val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
      df.format(ts)
    }
  }
}
