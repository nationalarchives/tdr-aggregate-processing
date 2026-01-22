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

  private sealed trait HardDriveProperty {
    val baseProperty: BaseProperty
  }

  private object NormalisePropertyValue {
    def normalise(id: String, value: Json): Json = id match {
      case HardDriveDateLastModified.baseProperty.id => HardDriveDateLastModified.normaliseFunction.apply(value)
      case HardDriveFilePath.baseProperty.id         => HardDriveFilePath.normaliseFunction.apply(value)
      case HardDriveClosureType.baseProperty.id | HardDriveTitleClosed.baseProperty.id | HardDriveDescriptionClosed.baseProperty.id | HardDriveClosurePeriod.baseProperty.id =>
        switchToAlternateValue(value)
      case _ => value
    }
  }

  private case object HardDriveFilePath extends HardDriveProperty {
    override val baseProperty: BaseProperty = FilePathProperty
    def normaliseFunction: Json => Json = (value: Json) => {
      val originalValue = value.asString.get
      val replaceBackSlashes = originalValue.replace("\\", "/")
      replaceBackSlashes.drop(replaceBackSlashes.indexOfSlice("content/")).mkString.asJson
    }
  }

  private case object HardDriveClosurePeriod extends HardDriveProperty {
    override val baseProperty: BaseProperty = ClosurePeriodProperty
  }

  private case object HardDriveClosureType extends HardDriveProperty {
    override val baseProperty: BaseProperty = ClosureTypeProperty
  }

  private case object HardDriveTitleClosed extends HardDriveProperty {
    override val baseProperty: BaseProperty = TitleClosedProperty
  }

  private case object HardDriveDescriptionClosed extends HardDriveProperty {
    override val baseProperty: BaseProperty = DescriptionClosedProperty
  }

  private case object HardDriveDateLastModified extends HardDriveProperty {
    override val baseProperty: BaseProperty = DateLastModifiedProperty
    def normaliseFunction: Json => Json = (value: Json) => {
      val originalValue = value.asString.get + "Z"
      t"$originalValue".getTime.toString.asJson
    }
  }

  private def switchToAlternateValue(value: Json): Json = {
    val originalValue = value.asString.get
    alternateValueMapping.getOrElse(originalValue.toLowerCase, originalValue).asJson
  }

  val metadataHandler = new BaseMetadataHandler(mapper, defaultPropertyValues, suppliedProperties, systemProperties, NormalisePropertyValue.normalise)
}
