package uk.gov.nationalarchives.aggregate.processing.modules

import graphql.codegen.types.ClientSideMetadataInput
import io.circe.syntax.EncoderOps
import io.circe.{Decoder, Json}

class BaseMetadataHandler(mapper: String => String, defaultProperties: Map[String, String],
                          normaliseFunction: (String, Json) => Json) extends MetadataHandler {
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

  override val sourceToBasePropertiesMapper: String => String = mapper
  override val defaultPropertyValues: Map[String, String] = defaultProperties

  def normaliseValues(property: String, value: Json): Json = {
    normaliseFunction(property, value)
  }

  def toMetadataProperties(json: Json, properties: Seq[String]): List[MetadataProperty] = {
    for {
      obj <- json.asObject.toList
      key <- properties
      value <- obj(key).flatMap(_.asString)
    } yield MetadataProperty(key, value)
  }

  def convertToBaseMetadata(sourceJson: Json): Json = {
    sourceJson.asObject.get.toMap
      .map(fv => {
        val originalField = fv._1
        val field = sourceToBasePropertiesMapper(originalField)
        field -> normaliseValues(field, fv._2)
      }).asJson
  }

  def toClientSideMetadataInput(baseMetadataJson: Json): Decoder.Result[ClientSideMetadataInput] =
    baseMetadataJson.as[ClientSideMetadataInput]
}
