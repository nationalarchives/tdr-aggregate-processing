package uk.gov.nationalarchives.aggregate.processing.modules

import io.circe.Json
import io.circe.syntax.EncoderOps
import uk.gov.nationalarchives.aggregate.processing.modules.SharePointMetadataHandler.NormaliseValueProperty.{FilePath, LastModified}
import uk.gov.nationalarchives.tdr.schemautils.ConfigUtils

import java.sql.Timestamp
import java.time.Instant
import scala.language.implicitConversions

class SharePointMetadataHandler(mapper: String => String) extends MetadataHandler {
  override val sourceToBasePropertiesMapper: String => String = mapper

  override def normaliseValues(property: String, value: Json): Json = {
    property match {
      case _ if property == FilePath.name     => FilePath.normalisedValue(value)
      case _ if property == LastModified.name => LastModified.normalisedValue(value)
      case _                                  => value
    }
  }
}

object SharePointMetadataHandler {
  implicit class StringTimeConversions(sc: StringContext) {
    def t(args: Any*): Timestamp =
      Timestamp.from(Instant.parse(sc.s(args: _*)))
  }

  private val metadataConfig: ConfigUtils.MetadataConfiguration = ConfigUtils.loadConfiguration
  private val mapper: String => String = metadataConfig.inputToPropertyMapper("sharePointTag")
  private case class SharePointLocationPath(root: String, site: String, library: String, filePath: String)

  private def sharePointLocationPathToFilePath(locationPath: String): SharePointLocationPath = {
    val pathComponents = locationPath.split("/")
    SharePointLocationPath(pathComponents(1), pathComponents(2), pathComponents(3), pathComponents.slice(1, pathComponents.length).mkString("/"))
  }

  object NormaliseValueProperty extends Enumeration {
    case class NormaliseValuePropertyType(name: String, normaliseFunction: Json => Json) extends super.Val {
      def normalisedValue(json: Json): Json = {
        normaliseFunction(json)
      }
    }
    val FilePath = NormaliseValuePropertyType(
      "file_path",
      (value: Json) => {
        val originalValue = value.asString.get
        sharePointLocationPathToFilePath(originalValue).filePath.asJson
      }
    )
    val LastModified = NormaliseValuePropertyType(
      "date_last_modified",
      (value: Json) => {
        val originalValue = value.asString.get
        t"$originalValue".getTime.toString.asJson
      }
    )
  }

  def apply() = new SharePointMetadataHandler(mapper)
}
