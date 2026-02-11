package uk.gov.nationalarchives.aggregate.processing.modules.assetprocessing.metadata

import graphql.codegen.types.ClientSideMetadataInput
import io.circe.{Decoder, Json}
import uk.gov.nationalarchives.aggregate.processing.modules.Common.MetadataClassification.MetadataClassification

case class MetadataProperty(propertyName: String, propertyValue: String)

sealed trait BaseProperty {
  val id: String
}

case object ClosurePeriodProperty extends BaseProperty {
  val id: String = "closure_period"
}

case object ClosureTypeProperty extends BaseProperty {
  val id: String = "closure_type"
}

case object DescriptionClosedProperty extends BaseProperty {
  val id: String = "description_closed"
}

case object ClientSideChecksumProperty extends BaseProperty {
  val id: String = "client_side_checksum"
}

case object DateLastModifiedProperty extends BaseProperty {
  val id: String = "date_last_modified"
}

case object FileNameProperty extends BaseProperty {
  val id: String = "file_name"
}

case object FilePathProperty extends BaseProperty {
  val id: String = "file_path"
}

case object FileSizeProperty extends BaseProperty {
  val id: String = "file_size"
}

case object FoiExemptionCodeProperty extends BaseProperty {
  val id: String = "foi_exemption_code"
}

case object MatchIdProperty extends BaseProperty {
  val id: String = "matchId"
}

case object TransferIdProperty extends BaseProperty {
  val id: String = "transferId"
}

case object TitleClosedProperty extends BaseProperty {
  val id: String = "title_closed"
}

trait MetadataHandler {
  val sourceToBasePropertiesMapper: String => String

  val defaultPropertyValues: Map[String, String]

  def toClientSideMetadataInput(baseMetadataJson: Json): Decoder.Result[ClientSideMetadataInput]

  def toMetadataProperties(json: Json, properties: Seq[String]): List[MetadataProperty]

  def convertToBaseMetadata(sourceJson: Json): Json

  def classifyBaseMetadata(baseMetadataJson: Json): Map[MetadataClassification, List[MetadataProperty]]
}
