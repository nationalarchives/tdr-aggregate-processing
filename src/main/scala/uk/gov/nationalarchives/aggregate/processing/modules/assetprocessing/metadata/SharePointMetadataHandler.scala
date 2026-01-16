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
  private lazy val tdrDataLoadHeaderToPropertyMapper: String => String = metadataConfig.propertyToOutputMapper("tdrFileHeader")
  private val suppliedProperties: Seq[String] = metadataConfig.getPropertiesByPropertyType(Supplied.toString).map(p => tdrDataLoadHeaderToPropertyMapper(p))
  private val systemProperties: Seq[String] = metadataConfig.getPropertiesByPropertyType(System.toString)
  private val mapper: String => String = metadataConfig.inputToPropertyMapper("sharePointTag")
  private val defaultPropertyValues: Map[String, String] = metadataConfig.getPropertiesWithDefaultValue
  private case class SharePointLocationPath(root: String, site: String, library: String, filePath: String)

  private def sharePointLocationPathToFilePath(locationPath: String): SharePointLocationPath = {
    val pathComponents = locationPath.split("/")
    SharePointLocationPath(pathComponents(1), pathComponents(2), pathComponents(3), pathComponents.slice(1, pathComponents.length).mkString("/"))
  }

  private sealed trait SharePointProperty {
    val baseProperty: BaseProperty
    def normaliseFunction: Json => Json
  }

  private object NormalisePropertyValue {
    def normalise(id: String, value: Json): Json = id match {
      case SharePointFilePath.baseProperty.id         => SharePointFilePath.normaliseFunction.apply(value)
      case SharePointDateLastModified.baseProperty.id => SharePointDateLastModified.normaliseFunction.apply(value)
      case _                                          => value
    }
  }

  private case object SharePointFilePath extends SharePointProperty {
    override val baseProperty: BaseProperty = FilePathProperty
    override def normaliseFunction: Json => Json = (value: Json) => {
      val originalValue = value.asString.get
      sharePointLocationPathToFilePath(originalValue).filePath.asJson
    }
  }

  private case object SharePointDateLastModified extends SharePointProperty {
    override val baseProperty: BaseProperty = DateLastModifiedProperty
    override def normaliseFunction: Json => Json = (value: Json) => {
      val originalValue = value.asString.get
      t"$originalValue".getTime.toString.asJson
    }
  }

  val metadataHandler = new BaseMetadataHandler(mapper, defaultPropertyValues, suppliedProperties, systemProperties, NormalisePropertyValue.normalise)
}
