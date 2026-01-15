package uk.gov.nationalarchives.aggregate.processing.modules

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.Logger
import io.circe.syntax.EncoderOps
import io.circe.{Encoder, Json}
import uk.gov.nationalarchives.aggregate.processing.AggregateProcessingLambda.AggregateProcessingError
import uk.gov.nationalarchives.aggregate.processing.modules.ErrorHandling.{BaseError, ErrorJson, config}
import uk.gov.nationalarchives.aggregate.processing.modules.assetprocessing.AssetProcessing.AssetProcessingError
import uk.gov.nationalarchives.aggregate.processing.modules.orchestration.TransferOrchestration.TransferError
import uk.gov.nationalarchives.aws.utils.s3.{S3Clients, S3Utils}

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.UUID

class ErrorHandling(s3Utils: S3Utils) {

  def handleError(error: BaseError, logger: Logger): Unit = {
    val bucket = config.getString("s3.transferErrorBucket")
    val errorId = UUID.randomUUID().toString
    val errorMessage = error.toString
    logger.error(errorMessage)

    error match {
      case ape: AssetProcessingError =>
        val key = s"${ape.consignmentId.getOrElse("unknown")}/${ape.simpleName}/$errorId.error"
        val errorJson = ErrorJson(ape.consignmentId.map(_.toString), ape.matchId, ape.source, errorId, ape.errorCode, ape.errorMsg)
        uploadErrorToS3(s3Utils, bucket, key, errorJson.asJson)
      case agpe: AggregateProcessingError =>
        val key = s"${agpe.consignmentId.getOrElse("unknown")}/${agpe.simpleName}/$errorId.error"
        val errorJson = ErrorJson(agpe.consignmentId.map(_.toString), None, None, errorId, agpe.errorCode, agpe.errorMessage)
        uploadErrorToS3(s3Utils, bucket, key, errorJson.asJson)
      case te: TransferError =>
        val key = s"${te.consignmentId.getOrElse("unknown")}/${te.simpleName}/$errorId.error"
        val errorJson = ErrorJson(te.consignmentId.map(_.toString), None, None, errorId, te.errorCode, te.errorMessage)
        uploadErrorToS3(s3Utils, bucket, key, errorJson.asJson)
      case error =>
        val key = s"unknown/${error.simpleName}/$errorId.error"
        val errorJson = ErrorJson(None, None, None, errorId, "UNKNOWN_ERROR", errorMessage)
        uploadErrorToS3(s3Utils, bucket, key, errorJson.asJson)
    }
  }

  private def uploadErrorToS3(
      s3Utils: S3Utils,
      bucket: String,
      key: String,
      json: Json
  ): Unit = {
    val file: Path = Files.createTempFile("error-file", ".error")
    Files.write(file, json.spaces2.getBytes(StandardCharsets.UTF_8))
    s3Utils.upload(bucket, key, file)
  }
}

object ErrorHandling {
  trait BaseError {
    val simpleName: String = this.getClass.getSimpleName
    val consignmentId: Option[UUID]
  }

  case class ErrorJson(
      transferId: Option[String],
      matchId: Option[String],
      source: Option[String],
      errorId: String,
      errorCode: String,
      errorMessage: String
  )

  implicit val errorJsonEncoder: Encoder[ErrorJson] = Encoder.forProduct6(
    "transferId",
    "matchId",
    "source",
    "errorId",
    "errorCode",
    "errorMessage"
  )(e => (e.transferId, e.matchId, e.source, e.errorId, e.errorCode, e.errorMessage))

  val config: Config = ConfigFactory.load()
  val s3Utils: S3Utils = S3Utils(S3Clients.s3Async(config.getString("s3.endpoint")))
  def apply() = new ErrorHandling(s3Utils)
}
