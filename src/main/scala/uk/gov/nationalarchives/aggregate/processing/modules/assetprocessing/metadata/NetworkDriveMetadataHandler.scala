package uk.gov.nationalarchives.aggregate.processing.modules.assetprocessing.metadata

import io.circe.Json
import io.circe.syntax.EncoderOps
import uk.gov.nationalarchives.aggregate.processing.modules.Common.MetadataClassification.{Supplied, System}
import uk.gov.nationalarchives.tdr.schemautils.ConfigUtils

object NetworkDriveMetadataHandler {
  private val metadataConfig: ConfigUtils.MetadataConfiguration = ConfigUtils.loadConfiguration
  private val mapper: String => String = metadataConfig.inputToPropertyMapper("networkDriveHeader")
  private val defaultPropertyValues: Map[String, String] = metadataConfig.getPropertiesWithDefaultValue
  private val suppliedProperties: Seq[String] = metadataConfig.getPropertiesByPropertyType(Supplied.toString)
  private val systemProperties: Seq[String] = metadataConfig.getPropertiesByPropertyType(System.toString)

  private def normaliseFilePath(value: Json): Json = {
    val originalValue = value.asString.get
    originalValue.replace("\\", "/").asJson
  }

  private object NormalisePropertyValue {
    def normalise(id: String, value: Json): Json = id match {
      case FilePathProperty.id => normaliseFilePath(value)
      case _                   => value
    }
  }

  private def enrichMetadata(baseMetadata: Map[String, Json]): Json = {
    val filePath = baseMetadata(FilePathProperty.id).asString.get
    val fileName = getFileName(filePath)
    baseMetadata.asJsonObject.add(FileNameProperty.id, fileName.asJson).asJson
  }

  private def getFileName(filePath: String): String = {
    filePath.split("/").last
  }

  val metadataHandler = new BaseMetadataHandler(mapper, defaultPropertyValues, suppliedProperties, systemProperties, NormalisePropertyValue.normalise, enrichMetadata)
}
