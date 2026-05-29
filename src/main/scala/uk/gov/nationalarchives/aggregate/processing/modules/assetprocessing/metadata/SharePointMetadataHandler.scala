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

  private def sharePointLocationPathToFilePath(
      locationPath: String,
      siteDisplayName: Option[Json],
      libraryDisplayName: Option[Json],
      ignoreSiteName: Boolean
  ): SharePointLocationPath = {
    val pathComponents = locationPath.split("/")
    val root = pathComponents(1)
    val siteName = if (siteDisplayName.nonEmpty) siteDisplayName.get.asString.get else pathComponents(2)
    val libraryName = if (libraryDisplayName.nonEmpty) libraryDisplayName.get.asString.get else pathComponents(3)
    val folderNames = pathComponents.slice(4, pathComponents.length).mkString("/")
    val filePath = if (ignoreSiteName) { s"$libraryName/$folderNames" }
    else s"$siteName/$libraryName/$folderNames"

    SharePointLocationPath(root, pathComponents(2), pathComponents(3), filePath)
  }

  private def normaliseFilePath(input: NormaliseValueInput): Json = {
    val jsonMap = input.allMetadataJson.toMap
    val siteName: Option[Json] = jsonMap.get("SiteName")
    val libraryName: Option[Json] = jsonMap.get("LibraryName")
    val originalValue = input.value.asString.get
    sharePointLocationPathToFilePath(originalValue, siteName, libraryName, input.ignoreSiteName).filePath.asJson
  }

  private def normaliseDateTime(value: Json): Json = {
    val originalValue = value.asString.get
    t"$originalValue".getTime.toString.asJson
  }

  private def normaliseNumber(value: Json): Json = {
    val originalValue = value.asNumber.get
    originalValue.toString.asJson
  }

  private object NormalisePropertyValue {
    def normalise(input: NormaliseValueInput): Json = input.property match {
      case FilePathProperty.id                                                                                              => normaliseFilePath(input)
      case DateLastModifiedProperty.id | ClosureStartDateProperty.id | EndDateProperty.id | FoiExemptionAssertedProperty.id => normaliseDateTime(input.value)
      case ClosurePeriodProperty.id                                                                                         => normaliseNumber(input.value)
      case _                                                                                                                => input.value
    }
  }

  val metadataHandler = new BaseMetadataHandler(mapper, defaultPropertyValues, suppliedProperties, systemProperties, NormalisePropertyValue.normalise)
}
