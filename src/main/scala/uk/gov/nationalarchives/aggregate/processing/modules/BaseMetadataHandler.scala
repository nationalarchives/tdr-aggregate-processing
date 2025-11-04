package uk.gov.nationalarchives.aggregate.processing.modules

import graphql.codegen.types.ClientSideMetadataInput
import io.circe.{Decoder, Json}
import io.circe.syntax.EncoderOps

import java.sql.Timestamp
import java.time.Instant

class BaseMetadataHandler(mapper: String => String, defaultProperties: Map[String, String], normaliseFunction: (String, Json) => Json) extends MetadataHandler {
  implicit class StringTimeConversions(sc: StringContext) {
    def t(args: Any*): Timestamp =
      Timestamp.from(Instant.parse(sc.s(args: _*)))
  }

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

  override def normaliseValues(property: String, value: Json): Json = {
    val x = normaliseFunction(property, value)
    x
  }

  def toMetadataProperties(json: Json, properties: Seq[String]): List[MetadataProperty] = {
    for {
      obj <- json.asObject.toList
      key <- properties
      value <- obj(key).flatMap(_.asString)
    } yield MetadataProperty(key, value)
  }

  private def addDefaultValues(sourceJson: Map[String, Json]): Json = {
    val defaults = defaultPropertyValues.iterator.flatMap(dp => {
      val defaultKey = dp._1
      val containsDefaultKey = sourceJson.contains(defaultKey) && sourceJson(defaultKey).asString.getOrElse("").nonEmpty
      lazy val defaultJsonValue = dp._2.asJson
      if (containsDefaultKey) {
        None
      } else {
        Some(defaultKey -> defaultJsonValue) }
    }).toMap
    (sourceJson ++ defaults).asJson
  }

  def convertToBaseMetadata(sourceJson: Json): Json = {
    val baseMetadataJson = sourceJson.asObject.get.toMap
      .map(fv => {
        val originalField = fv._1
        val field = sourceToBasePropertiesMapper(originalField)
        field -> normaliseValues(field, fv._2)
      })
    addDefaultValues(baseMetadataJson).asJson
  }

  def toClientSideMetadataInput(baseMetadataJson: Json): Decoder.Result[ClientSideMetadataInput] =
    baseMetadataJson.as[ClientSideMetadataInput]
}
