package uk.gov.nationalarchives.aggregate.processing.modules

import io.circe.Json
import io.circe.syntax.EncoderOps
import uk.gov.nationalarchives.aggregate.processing.modules
import uk.gov.nationalarchives.tdr.schemautils.ConfigUtils

import java.sql.Timestamp
import java.time.Instant

object DroidMetadataHandler {
  implicit class StringTimeConversions(sc: StringContext) {
    def t(args: Any*): Timestamp =
      Timestamp.from(Instant.parse(sc.s(args: _*)))
  }

  private val metadataConfig: ConfigUtils.MetadataConfiguration = ConfigUtils.loadConfiguration
  private val mapper: String => String = metadataConfig.inputToPropertyMapper("droidHeader")
  private val defaultPropertyValues: Map[String, String] = metadataConfig.getPropertiesWithDefaultValue

  private sealed trait DroidProperty {
    val baseProperty: BaseProperty
    def normaliseFunction: Json => Json
  }

  object NormalisePropertyValue {
    def normalise(id: String, value: Json): Json = id match {
      case DroidFilePath.baseProperty.id         => DroidFilePath.normaliseFunction.apply(value)
      case DroidDateLastModified.baseProperty.id => DroidDateLastModified.normaliseFunction.apply(value)
      case _                                     => value
    }
  }

  private case object DroidFilePath extends DroidProperty {
    override val baseProperty: BaseProperty = modules.FilePathProperty
    override def normaliseFunction: Json => Json = (value: Json) => {
      val originalValue = value.asString.get
      val replaceBackSlashes = originalValue.replace("\\", "/")
      replaceBackSlashes.drop(replaceBackSlashes.indexOfSlice("Content/")).mkString.asJson
    }
  }

  private case object DroidDateLastModified extends DroidProperty {
    override val baseProperty: BaseProperty = modules.DateLastModifiedProperty
    override def normaliseFunction: Json => Json = (value: Json) => {
      val originalValue = value.asString.get + "Z"
      t"$originalValue".getTime.toString.asJson
    }
  }

  def apply() = new BaseMetadataHandler(mapper, defaultPropertyValues, NormalisePropertyValue.normalise)
}
