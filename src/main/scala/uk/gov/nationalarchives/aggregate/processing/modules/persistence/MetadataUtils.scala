package uk.gov.nationalarchives.aggregate.processing.modules.persistence

import cats.effect.IO
import doobie.implicits._
import doobie.util.transactor.Transactor.Aux
import doobie.Transactor
import doobie.postgres.implicits._
import io.circe.generic.semiauto.deriveDecoder
import io.circe.{Decoder, Json}
import io.circe.syntax.EncoderOps
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.rds.RdsUtilities
import software.amazon.awssdk.services.rds.model.GenerateAuthenticationTokenRequest
import uk.gov.nationalarchives.aggregate.processing.modules.persistence.Model.AssetData
import uk.gov.nationalarchives.tdr.schemautils.ConfigUtils

import java.util.UUID

class MetadataUtils {
  implicit val dataLoadInputDecoder: Decoder[DataLoadInput] = deriveDecoder[DataLoadInput]

  private val schemaConfig = ConfigUtils.loadConfiguration
  private val port = 12345
  private val hostName = "placeholder"
  private val dbUser = "placeholder"
  private val dbName = "placeholder"
  private def dbPassword: String = {
      val rdsClient = RdsUtilities.builder().region(Region.EU_WEST_2).build()
      val request = GenerateAuthenticationTokenRequest
        .builder()
        .credentialsProvider(DefaultCredentialsProvider.builder().build())
        .hostname(hostName)
        .port(port)
        .username(dbUser)
        .region(Region.EU_WEST_2)
        .build()
      rdsClient.generateAuthenticationToken(request)
  }

  private val transactor: Aux[IO, Unit] = {
    val certificatePath = getClass.getResource("/rds-eu-west-2-bundle.pem").getPath
    val suffix = s"?ssl=true&sslrootcert=$certificatePath&sslmode=verify-full"
    val jdbcUrl = s"jdbc:postgresql://$hostName:$port/$dbName$suffix"
    Transactor.fromDriverManager[IO](
      driver = "org.postgresql.Driver",
      url = jdbcUrl,
      user = dbUser,
      password = dbPassword,
      logHandler = None
    )
  }

  case class DataLoadInput(FileType: String, Filename: String, FileReference: String, ParentReference: Option[String])

  private val dataLoadMapper = schemaConfig.propertyToOutputMapper("tdrDataLoadHeader")

  private def convertToTdrDataLoad(sourceJson: Json) = {
    sourceJson.asObject.get.toMap.map(fv => {
      val originalField = fv._1
      val field = dataLoadMapper(originalField)
      field -> fv._2
    }).asJson
  }

  def insertAssetData(inputs: List[AssetData]): IO[List[Int]] = {

    def fileInsert(
                    fileId: UUID, consignmentId: UUID, userId: UUID, fileType: String, fileName: String, fileRef: String, parentRef: String, uploadMatchId: String): doobie.ConnectionIO[Int] =
      sql"""INSERT INTO "File" ("FileId", "ConsignmentId", "UserId", "FileType", "FileName", "FileReference", "ParentReference", "UploadMatchId") values ($fileId, $consignmentId, $userId, $fileType, $fileName, $fileRef, $parentRef, $uploadMatchId)"""
        .update.run

    inputs.map(input => {
      val fileId = input.assetId
      val consignmentId = input.consignmentId
      val userId = input.userId
      val matchId = input.matchId
      val dataLoadInput = convertToTdrDataLoad(input.input.asJson).as[DataLoadInput].fold(
        err => throw new RuntimeException(err.getMessage()),
        input => input)

      fileInsert(fileId, consignmentId, userId, dataLoadInput.FileType, dataLoadInput.Filename, dataLoadInput.FileReference, dataLoadInput.ParentReference.getOrElse(""), matchId).transact(transactor)
    }).sequence
  }
}

object MetadataUtils {
  def apply(): IO[MetadataUtils] = IO(new MetadataUtils())
}
