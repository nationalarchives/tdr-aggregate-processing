package uk.gov.nationalarchives.aggregate.processing.modules.assetprocessing.initialchecks

import graphql.codegen.types.ClientSideMetadataInput
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar.{mock, when}
import uk.gov.nationalarchives.aggregate.processing.ExternalServiceSpec
import uk.gov.nationalarchives.aggregate.processing.config.ApplicationConfig
import uk.gov.nationalarchives.aggregate.processing.modules.assetprocessing.AssetProcessing.{AssetProcessingError, AssetProcessingEvent}
import uk.gov.nationalarchives.aws.utils.s3.S3Utils
import uk.gov.nationalarchives.tdr.common.utils.objectkeycontext.AssetSources.SharePoint
import uk.gov.nationalarchives.tdr.common.utils.objectkeycontext.ObjectTypes.Metadata
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.util.UUID

class AssetObjectChecksSpec extends ExternalServiceSpec {
  private val appConfigMock = mock[ApplicationConfig.type]
  private val s3UtilsMock = mock[S3Utils]

  val transferId: UUID = UUID.randomUUID()
  val event = AssetProcessingEvent(
    UUID.randomUUID(),
    transferId,
    "matchId-1",
    SharePoint,
    Metadata,
    "s3-source-bucket",
    "object/key",
    ignoreSiteName = false,
    "",
    ""
  )

  "runCheck" should "return an error when the event match id does not match the metadata match id field value" in {
    when(s3UtilsMock.objectKeyExists(any[String], any[String])).thenReturn(true)
    when(appConfigMock.blockCheckRecordExists).thenReturn(false)

    val input = ClientSideMetadataInput("folder1/folder2/file.txt", "checksum", 12L, 0, "different-match-id")

    val check = new AssetObjectChecks(s3UtilsMock, appConfigMock)
    val result = check.runCheck(event, input)

    result.size shouldBe 1
    checkMismatchedMatchIdsError(result.head)
  }

  "runCheck" should "return an error when a record S3 object does not exist for given match id" in {
    when(s3UtilsMock.objectKeyExists(any[String], any[String])).thenReturn(false)
    when(appConfigMock.blockCheckRecordExists).thenReturn(false)

    val input = ClientSideMetadataInput("folder1/folder2/file.txt", "checksum", 12L, 0, "matchId-1")

    val check = new AssetObjectChecks(s3UtilsMock, appConfigMock)
    val result = check.runCheck(event, input)

    result.size shouldBe 1
    checkMissingRecordError(result.head)
  }

  "runCheck" should "return no errors when a record S3 object does not exist for given match id but FAB set to true" in {
    when(s3UtilsMock.objectKeyExists(any[String], any[String])).thenReturn(false)
    when(appConfigMock.blockCheckRecordExists).thenReturn(true)

    val input = ClientSideMetadataInput("folder1/folder2/file.txt", "checksum", 12L, 0, "matchId-1")

    val check = new AssetObjectChecks(s3UtilsMock, appConfigMock)
    val result = check.runCheck(event, input)

    result.size shouldBe 0
  }

  "runCheck" should "return multiple errors when multiple checks fail" in {
    when(s3UtilsMock.objectKeyExists(any[String], any[String])).thenReturn(false)
    when(appConfigMock.blockCheckRecordExists).thenReturn(false)

    val input = ClientSideMetadataInput("folder1/folder2/file.txt", "checksum", 12L, 0, "different-match-id")

    val check = new AssetObjectChecks(s3UtilsMock, appConfigMock)
    val result = check.runCheck(event, input)

    result.size shouldBe 2
    checkMismatchedMatchIdsError(result.head)
    checkMissingRecordError(result.last)
  }

  "runCheck" should "return no errors when all checks pass" in {
    when(s3UtilsMock.objectKeyExists(any[String], any[String])).thenReturn(true)
    when(appConfigMock.blockCheckRecordExists).thenReturn(false)

    val input = ClientSideMetadataInput("folder1/folder2/file.txt", "checksum", 12L, 0, "matchId-1")

    val check = new AssetObjectChecks(s3UtilsMock, appConfigMock)
    val result = check.runCheck(event, input)

    result.size shouldBe 0
  }

  private def checkMismatchedMatchIdsError(error: AssetProcessingError) = {
    error.matchId shouldBe None
    error.errorCode shouldBe "INITIAL_CHECKS.MATCH_ID.MISMATCH"
    error.errorMsg shouldBe "Mismatched match ids: matchId-1 and different-match-id"
    error.source.get shouldBe "sharepoint"
    error.consignmentId.get shouldBe transferId
  }

  private def checkMissingRecordError(error: AssetProcessingError) = {
    error.matchId shouldBe None
    error.errorCode shouldBe "INITIAL_CHECKS.MISSING.RECORD"
    error.errorMsg shouldBe "Record does not exist: matchId-1"
    error.source.get shouldBe "sharepoint"
    error.consignmentId.get shouldBe transferId
  }
}
