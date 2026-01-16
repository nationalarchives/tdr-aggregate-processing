package uk.gov.nationalarchives.aggregate.processing.modules

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.Logger
import io.circe.generic.semiauto.deriveEncoder
import io.circe.syntax.EncoderOps
import io.circe.{Encoder, Json}
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import uk.gov.nationalarchives.aggregate.processing.AggregateProcessingLambda.AggregateProcessingError
import uk.gov.nationalarchives.aggregate.processing.modules.ErrorHandling.{BaseError, config}
import uk.gov.nationalarchives.aggregate.processing.modules.assetprocessing.AssetProcessing.AssetProcessingError
import uk.gov.nationalarchives.aggregate.processing.modules.orchestration.TransferOrchestration.TransferError
import uk.gov.nationalarchives.aws.utils.s3.S3Clients

import java.nio.charset.StandardCharsets
import java.util.UUID

class ErrorHandling(s3AsyncClient: S3AsyncClient) {

  def handleError(error: BaseError, logger: Logger): Unit = {
    val bucket = config.getString("s3.transferErrorBucket")
    val errorId = UUID.randomUUID().toString
    val errorMessage = error.toString
    logger.error(errorMessage)

    val errorKey = s"${error.consignmentId.getOrElse("unknown")}/${error.simpleName}/$errorId.error"
    uploadErrorToS3(s3AsyncClient, bucket, errorKey, error.asJson)
  }

  private def uploadErrorToS3(
      s3AsyncClient: S3AsyncClient,
      bucket: String,
      key: String,
      json: Json
  ): Unit = {
    // TODO add this logic to the tdr-aws-utils repo
    val jsonBytes = json.spaces2.getBytes(StandardCharsets.UTF_8)
    val asyncBodyRequest = AsyncRequestBody.fromBytes(jsonBytes)
    val putObjectRequest = PutObjectRequest
      .builder()
      .key(key)
      .bucket(bucket)
      .build()
    s3AsyncClient.putObject(putObjectRequest, asyncBodyRequest)
  }
}

object ErrorHandling {
  trait BaseError {
    val simpleName: String = this.getClass.getSimpleName
    val consignmentId: Option[UUID]
  }

  implicit val encodeAggregateProcessingError: Encoder[AggregateProcessingError] = deriveEncoder[AggregateProcessingError]
  implicit val encodeAssetProcessingError: Encoder[AssetProcessingError] = deriveEncoder[AssetProcessingError]
  implicit val encodeTransferError: Encoder[TransferError] = deriveEncoder[TransferError]

  implicit val encoderBaseError: Encoder[BaseError] = Encoder.instance {
    case e: AggregateProcessingError => e.asJson
    case e: AssetProcessingError     => e.asJson
    case e: TransferError            => e.asJson
    case _                           => throw new RuntimeException("Unknown BaseError type")
  }

  val config: Config = ConfigFactory.load()
  val s3AsyncClient: S3AsyncClient = S3Clients.s3Async(config.getString("s3.endpoint"))
  def apply() = new ErrorHandling(s3AsyncClient: S3AsyncClient)
}
