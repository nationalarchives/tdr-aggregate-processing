package uk.gov.nationalarchives.aggregate.processing.modules.assetprocessing.metadata

import io.circe.Json
import io.circe.syntax.EncoderOps
import uk.gov.nationalarchives.aggregate.processing.modules.Common.MetadataClassification.{Supplied, System}
import uk.gov.nationalarchives.tdr.schemautils.ConfigUtils

import java.sql.Timestamp
import java.time.Instant
import scala.language.implicitConversions

object SharePointMetadataHandler {
  implicit class StringTimeConversions(sc: StringContext) {
    def t(args: Any*): Timestamp =
      Timestamp.from(Instant.parse(sc.s(args: _*)))
  }

  private val metadataConfig: ConfigUtils.MetadataConfiguration = ConfigUtils.loadConfiguration
  private val suppliedProperties: Seq[String] = metadataConfig.getPropertiesByPropertyType(Supplied.toString)
  private val systemProperties: Seq[String] = metadataConfig.getPropertiesByPropertyType(System.toString)
  private val mapper: String => String = metadataConfig.inputToPropertyMapper("sharePointTag")
  private val defaultPropertyValues: Map[String, String] = metadataConfig.getPropertiesWithDefaultValue
  private case class SharePointLocationPath(root: String, site: String, library: String, filePath: String)

  private def sharePointLocationPathToFilePath(locationPath: String): SharePointLocationPath = {
    val pathComponents = locationPath.split("/")
    SharePointLocationPath(pathComponents(1), pathComponents(2), pathComponents(3), pathComponents.slice(1, pathComponents.length).mkString("/"))
  }

  private def normaliseFilePath(value: Json): Json = {
    val originalValue = value.asString.get
    sharePointLocationPathToFilePath(originalValue).filePath.asJson
  }

  private def normaliseDateTime(value: Json): Json = {
    val originalValue = value.asString.get
    t"$originalValue".getTime.toString.asJson
  }

  private def normaliseDateOnly(value: Json): Json = {
    val dateTimeValue = value.asString.get + "T00:00:00Z"
    t"$dateTimeValue".getTime.toString.asJson
  }

  private object NormalisePropertyValue {
    def normalise(id: String, value: Json): Json = id match {
      case FilePathProperty.id                                                           => normaliseFilePath(value)
      case DateLastModifiedProperty.id                                                   => normaliseDateTime(value)
      case ClosureStartDateProperty.id | EndDateProperty.id | FoiScheduleDateProperty.id => normaliseDateOnly(value)
      case _                                                                             => value
    }
  }

  val metadataHandler = new BaseMetadataHandler(mapper, defaultPropertyValues, suppliedProperties, systemProperties, NormalisePropertyValue.normalise)
}
