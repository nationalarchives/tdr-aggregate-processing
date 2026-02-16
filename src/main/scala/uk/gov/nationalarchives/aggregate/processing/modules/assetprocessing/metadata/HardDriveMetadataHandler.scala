package uk.gov.nationalarchives.aggregate.processing.modules.assetprocessing.metadata

import io.circe.Json
import io.circe.syntax.EncoderOps
import uk.gov.nationalarchives.aggregate.processing.modules.Common.MetadataClassification.{Supplied, System}
import uk.gov.nationalarchives.tdr.schemautils.ConfigUtils

import java.sql.Timestamp
import java.time.Instant

object HardDriveMetadataHandler {
  implicit class StringTimeConversions(sc: StringContext) {
    def t(args: Any*): Timestamp =
      Timestamp.from(Instant.parse(sc.s(args: _*)))
  }

  private val alternateValueMapping: Map[String, String] = Map(
    "0" -> "",
    "open_on_transfer" -> "Open",
    "closed_on_transfer" -> "Closed",
    "true" -> "false",
    "false" -> "true"
  )

  private val metadataConfig: ConfigUtils.MetadataConfiguration = ConfigUtils.loadConfiguration
  private val mapper: String => String = metadataConfig.inputToPropertyMapper("hardDriveHeader")
  private val defaultPropertyValues: Map[String, String] = metadataConfig.getPropertiesWithDefaultValue
  private val suppliedProperties: Seq[String] = metadataConfig.getPropertiesByPropertyType(Supplied.toString)
  private val systemProperties: Seq[String] = metadataConfig.getPropertiesByPropertyType(System.toString)

  private def normaliseDateTime(value: Json): Json = {
    val originalValue = value.asString.get + "Z"
    t"$originalValue".getTime.toString.asJson
  }

  private def normaliseFilePath(value: Json): Json = {
    val originalValue = value.asString.get
    val replaceBackSlashes = originalValue.replace("\\", "/")
    replaceBackSlashes.drop(replaceBackSlashes.indexOfSlice("content/")).mkString.asJson
  }

  private def normaliseFoiExemptionCode(value: Json): Json = {
    val originalValue = value.asString.get
    if (originalValue.toLowerCase == "open") {
      "".asJson
    } else originalValue.asJson
  }

  private def switchToAlternateValue(value: Json): Json = {
    val originalValue = value.asString.get
    alternateValueMapping.getOrElse(originalValue.toLowerCase, originalValue).asJson
  }

  private object NormalisePropertyValue {
    def normalise(id: String, value: Json): Json = id match {
      case DateLastModifiedProperty.id => normaliseDateTime(value)
      case FilePathProperty.id         => normaliseFilePath(value)
      case FoiExemptionCodeProperty.id => normaliseFoiExemptionCode(value)
      case ClosureTypeProperty.id | TitleClosedProperty.id | DescriptionClosedProperty.id | ClosurePeriodProperty.id =>
        switchToAlternateValue(value)
      case _ => value
    }
  }

  val metadataHandler = new BaseMetadataHandler(mapper, defaultPropertyValues, suppliedProperties, systemProperties, NormalisePropertyValue.normalise)
}
