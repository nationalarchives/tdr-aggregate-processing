package uk.gov.nationalarchives.aggregate.processing.modules

import graphql.codegen.types.ClientSideMetadataInput
import io.circe.syntax.EncoderOps
import io.circe.{Decoder, Json}

case class MetadataProperty(propertyName: String, propertyValue: String)

trait MetadataHandler {
  private object ClientSideMetadataInputProperty extends Enumeration {
    type ClientSideMetadataInputProperty = Value
    val FilePath: Value = Value("file_path")
    val Checksum: Value = Value("client_side_checksum")
    val LastModified: Value = Value("date_last_modified")
    val FileSize: Value = Value("file_size")
    val MatchId: Value = Value("matchId")
  }

  implicit val decodeClientSideInput: Decoder[ClientSideMetadataInput] =
    Decoder.instance[ClientSideMetadataInput] { c =>
      for {
        path <- c.downField(ClientSideMetadataInputProperty.FilePath.toString).as[String]
        checksum <- c.downField(ClientSideMetadataInputProperty.Checksum.toString).as[String]
        modified <- c.downField(ClientSideMetadataInputProperty.LastModified.toString).as[Long]
        fileSize <- c.downField(ClientSideMetadataInputProperty.FileSize.toString).as[Long]
        matchId <- c.downField(ClientSideMetadataInputProperty.MatchId.toString).as[String]
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
