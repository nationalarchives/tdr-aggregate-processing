package uk.gov.nationalarchives.aggregate.processing.modules.assetprocessing.metadata

import graphql.codegen.types.ClientSideMetadataInput
import io.circe.syntax.EncoderOps
import io.circe.{Decoder, Json, JsonObject}
import uk.gov.nationalarchives.aggregate.processing.modules.Common.MetadataClassification
import uk.gov.nationalarchives.aggregate.processing.modules.Common.MetadataClassification.MetadataClassification
import uk.gov.nationalarchives.tdr.schemautils.ConfigUtils

class BaseMetadataHandler(
    mapper: String => String,
    defaultProperties: Map[String, String],
    suppliedProperties: Seq[String],
    systemProperties: Seq[String],
    normaliseFunction: NormaliseValueInput => Json,
    enrichMetadataFunction: Map[String, Json] => Json = (baseMetadata: Map[String, Json]) => baseMetadata.asJson
) extends MetadataHandler {
  private val excludeProperties = suppliedProperties ++ systemProperties :+ MatchIdProperty.id :+ TransferIdProperty.id
  private val metadataConfig: ConfigUtils.MetadataConfiguration = ConfigUtils.loadConfiguration
  private val baseToTdrHeaderMapper = metadataConfig.propertyToOutputMapper("tdrFileHeader")

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

  def normaliseValues(input: NormaliseValueInput): Json = {
    normaliseFunction(input)
  }

  def toMetadataProperties(json: Json, properties: Seq[String]): List[MetadataProperty] = {
    for {
      obj <- json.asObject.toList
      key <- properties
      value <- obj(key).flatMap(_.asString)
    } yield MetadataProperty(key, value)
  }

  def convertToBaseMetadata(sourceJson: Json, ignoreSiteName: Boolean): Json = {
    val allMetadata: JsonObject = sourceJson.deepDropNullValues.asObject.get
    val metadata = allMetadata.toMap
      .map(fv => {
        val originalField = fv._1
        val field = sourceToBasePropertiesMapper(originalField)
        field -> normaliseValues(NormaliseValueInput(field, fv._2, allMetadata))
      })
    enrichMetadataFunction(metadata)
  }

  def toClientSideMetadataInput(baseMetadataJson: Json): Decoder.Result[ClientSideMetadataInput] =
    baseMetadataJson.as[ClientSideMetadataInput]

  def classifyBaseMetadata(baseMetadataJson: Json): Map[MetadataClassification, List[MetadataProperty]] = {
    val allPropertyNames: Seq[String] = baseMetadataJson.asObject.map(_.keys.toSeq).getOrElse(Seq.empty)
    val customProperties = allPropertyNames.diff(excludeProperties)
    val suppliedMetadata = toMetadataProperties(baseMetadataJson, suppliedProperties)
    val systemMetadata = toMetadataProperties(baseMetadataJson, systemProperties)
    val customMetadata = toMetadataProperties(baseMetadataJson, customProperties)
    Map(
      MetadataClassification.Custom -> customMetadata,
      MetadataClassification.Supplied -> convertBaseSuppliedToTdrHeaders(suppliedMetadata),
      MetadataClassification.System -> systemMetadata
    )
  }

  private def convertBaseSuppliedToTdrHeaders(suppliedProperties: List[MetadataProperty]): List[MetadataProperty] = {
    suppliedProperties.map(p => {
      val tdrHeader = baseToTdrHeaderMapper(p.propertyName)
      p.copy(propertyName = tdrHeader)
    })
  }
}
