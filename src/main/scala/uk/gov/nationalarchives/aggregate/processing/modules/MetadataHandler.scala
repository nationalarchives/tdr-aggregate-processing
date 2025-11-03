package uk.gov.nationalarchives.aggregate.processing.modules

import graphql.codegen.types.ClientSideMetadataInput
import io.circe.syntax.EncoderOps
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
  implicit val decodeClientSideInput: Decoder[ClientSideMetadataInput] =
    Decoder.instance[ClientSideMetadataInput] { c =>
      for {
        path <- c.downField(FilePathProperty.id).as[String]
        checksum <- c.downField(ClientSideChecksumProperty.id).as[String]
        modified <- c.downField(DateLastModifiedProperty.id).as[Long]
        fileSize <- c.downField(FileSizeProperty.id).as[Long]
        matchId <- c.downField(MatchIdProperty.id).as[String]
      } yield {
        new ClientSideMetadataInput(path, checksum, modified, fileSize, matchId)
      }
    }

  val sourceToBasePropertiesMapper: String => String

  def toClientSideMetadataInput(baseMetadataJson: Json): Decoder.Result[ClientSideMetadataInput] = baseMetadataJson.as[ClientSideMetadataInput]

  def toMetadataProperties(json: Json, properties: Seq[String]): List[MetadataProperty] = {
    for {
      obj <- json.asObject.toList
      key <- properties
      value <- obj(key).flatMap(_.asString)
    } yield MetadataProperty(key, value)
  }

  def normaliseValues(str: String, json: Json): Json

  def convertToBaseMetadata(sourceJson: Json): Json = {
    sourceJson.asObject.get.toMap
      .map(fv => {
        val originalField = fv._1
        val field = sourceToBasePropertiesMapper(originalField)
        field -> normaliseValues(field, fv._2)
      })
      .asJson
  }
}
