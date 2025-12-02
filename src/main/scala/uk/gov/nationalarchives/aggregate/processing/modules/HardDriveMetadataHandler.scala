package uk.gov.nationalarchives.aggregate.processing.modules

import io.circe.Json
import io.circe.syntax.EncoderOps
import uk.gov.nationalarchives.aggregate.processing.modules
import uk.gov.nationalarchives.tdr.schemautils.ConfigUtils

import java.sql.Timestamp
import java.time.Instant

object HardDriveMetadataHandler {
  implicit class StringTimeConversions(sc: StringContext) {
    def t(args: Any*): Timestamp =
      Timestamp.from(Instant.parse(sc.s(args: _*)))
  }

  private val metadataConfig: ConfigUtils.MetadataConfiguration = ConfigUtils.loadConfiguration
  private val mapper: String => String = metadataConfig.inputToPropertyMapper("hardDriveHeader")
  private val defaultPropertyValues: Map[String, String] = metadataConfig.getPropertiesWithDefaultValue

  private sealed trait HardDriveProperty {
    val baseProperty: BaseProperty
    def normaliseFunction: Json => Json
  }

  private object NormalisePropertyValue {
    def normalise(id: String, value: Json): Json = id match {
      case HardDriveDateLastModified.baseProperty.id  => HardDriveDateLastModified.normaliseFunction.apply(value)
      case HardDriveFilePath.baseProperty.id          => HardDriveFilePath.normaliseFunction.apply(value)
      case HardDriveClosureType.baseProperty.id       => HardDriveClosureType.normaliseFunction.apply(value)
      case HardDriveTitleClosed.baseProperty.id       => HardDriveTitleClosed.normaliseFunction.apply(value)
      case HardDriveDescriptionClosed.baseProperty.id => HardDriveDescriptionClosed.normaliseFunction.apply(value)
      case HardDriveClosurePeriod.baseProperty.id     => HardDriveClosurePeriod.normaliseFunction.apply(value)
      case _                                          => value
    }
  }

  private case object HardDriveFilePath extends HardDriveProperty {
    override val baseProperty: BaseProperty = modules.FilePathProperty
    override def normaliseFunction: Json => Json = (value: Json) => {
      val originalValue = value.asString.get
      val replaceBackSlashes = originalValue.replace("\\", "/")
      replaceBackSlashes.drop(replaceBackSlashes.indexOfSlice("content/")).mkString.asJson
    }
  }

  private case object HardDriveClosurePeriod extends HardDriveProperty {
    override val baseProperty: BaseProperty = modules.ClosurePeriodProperty
    override def normaliseFunction: Json => Json = (value: Json) => {
      val originalValue = value.asString.get
      originalValue match {
        case "0" => "".asJson
        case _   => value
      }
    }
  }

  private case object HardDriveClosureType extends HardDriveProperty {
    override val baseProperty: BaseProperty = modules.ClosureTypeProperty
    override def normaliseFunction: Json => Json = (value: Json) => {
      val originalValue = value.asString.get
      originalValue match {
        case "open_on_transfer" => "open".asJson
        case _                  => value
      }
    }
  }

  private case object HardDriveTitleClosed extends HardDriveProperty {
    override val baseProperty: BaseProperty = modules.TitleClosedProperty
    override def normaliseFunction: Json => Json = (value: Json) => {
      val originalValue = value.asString.get
      switchTrueFalse(originalValue).asJson
    }
  }

  private case object HardDriveDescriptionClosed extends HardDriveProperty {
    override val baseProperty: BaseProperty = modules.DescriptionClosedProperty
    override def normaliseFunction: Json => Json = (value: Json) => {
      val originalValue = value.asString.get
      switchTrueFalse(originalValue).asJson
    }
  }

  private case object HardDriveDateLastModified extends HardDriveProperty {
    override val baseProperty: BaseProperty = modules.DateLastModifiedProperty
    override def normaliseFunction: Json => Json = (value: Json) => {
      val originalValue = value.asString.get + "Z"
      t"$originalValue".getTime.toString.asJson
    }
  }

  private def switchTrueFalse(value: String): String = {
    value.toLowerCase match {
      case "true"  => "false"
      case "false" => "true"
    }
  }

  val metadataHandler = new BaseMetadataHandler(mapper, defaultPropertyValues, NormalisePropertyValue.normalise)
}
