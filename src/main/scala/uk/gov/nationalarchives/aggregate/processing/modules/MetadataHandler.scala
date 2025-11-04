package uk.gov.nationalarchives.aggregate.processing.modules

import graphql.codegen.types.ClientSideMetadataInput
import io.circe.{Decoder, Json}

case class MetadataProperty(propertyName: String, propertyValue: String)

sealed trait BaseProperty {
  val id: String
}

case object FilePathProperty extends BaseProperty {
  val id: String = "file_path"
}

case object ClientSideChecksumProperty extends BaseProperty {
  val id: String = "client_side_checksum"
}

case object DateLastModifiedProperty extends BaseProperty {
  val id: String = "date_last_modified"
}

case object FileSizeProperty extends BaseProperty {
  val id: String = "file_size"
}

case object MatchIdProperty extends BaseProperty {
  val id: String = "matchId"
}

trait MetadataHandler {
  val sourceToBasePropertiesMapper: String => String

  val defaultPropertyValues: Map[String, String]

  def toClientSideMetadataInput(baseMetadataJson: Json): Decoder.Result[ClientSideMetadataInput]

  def toMetadataProperties(json: Json, properties: Seq[String]): List[MetadataProperty]

  def normaliseValues(str: String, json: Json): Json

  def convertToBaseMetadata(sourceJson: Json): Json
}
