package uk.gov.nationalarchives.aggregate.processing.modules.assetprocessing.initialchecks

import graphql.codegen.types.ClientSideMetadataInput
import uk.gov.nationalarchives.aggregate.processing.config.ApplicationConfig
import uk.gov.nationalarchives.aggregate.processing.modules.Common.ProcessErrorType.{MatchIdError, MissingError}
import uk.gov.nationalarchives.aggregate.processing.modules.Common.ProcessErrorValue.{Mismatch, Record}
import uk.gov.nationalarchives.aggregate.processing.modules.Common.ProcessType.InitialChecks
import uk.gov.nationalarchives.aggregate.processing.modules.assetprocessing.AssetProcessing.{AssetProcessingError, AssetProcessingEvent}
import uk.gov.nationalarchives.aws.utils.s3.S3Utils

class AssetObjectChecks(s3Utils: S3Utils, appConfig: ApplicationConfig.type) extends InitialCheck {
  private val mismatchedMatchIdErrorCode = s"$InitialChecks.$MatchIdError.$Mismatch"
  private val missingRecordErrorCode = s"$InitialChecks.$MissingError.$Record"

  val errorCodes: Set[String] = Set(missingRecordErrorCode, mismatchedMatchIdErrorCode)

  def error(errorCode: String, event: AssetProcessingEvent, message: String): AssetProcessingError =
    AssetProcessingError(
      Some(event.consignmentId),
      None,
      Some(event.source.id),
      errorCode,
      message
    )

  def runCheck(event: AssetProcessingEvent, input: ClientSideMetadataInput): List[AssetProcessingError] =
    List(
      Option.when(event.matchId != input.matchId) {
        error(mismatchedMatchIdErrorCode, event, s"Mismatched match ids: ${event.matchId} and ${input.matchId}")
      },
      Option.when(
        !appConfig.blockCheckRecordExists &&
          !s3Utils.objectKeyExists(event.recordsS3Bucket, s"${event.recordsKeyPrefix}/${event.matchId}")
      ) {
        error(missingRecordErrorCode, event, s"Record does not exist: ${event.matchId}")
      }
    ).flatten
}

object AssetObjectChecks {
  def apply(s3Utils: S3Utils, appConfig: ApplicationConfig.type) = new AssetObjectChecks(s3Utils, appConfig)
}
