package uk.gov.nationalarchives.aggregate.processing.modules.assetprocessing.initialchecks

import graphql.codegen.types.ClientSideMetadataInput
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import uk.gov.nationalarchives.aggregate.processing.ExternalServiceSpec
import uk.gov.nationalarchives.aggregate.processing.modules.assetprocessing.AssetProcessing.AssetProcessingEvent
import uk.gov.nationalarchives.tdr.common.utils.objectkeycontext.AssetSources.SharePoint
import uk.gov.nationalarchives.tdr.common.utils.objectkeycontext.ObjectTypes.Metadata

import java.util.UUID

class FileSizeCheckSpec extends ExternalServiceSpec {
  val transferId: UUID = UUID.randomUUID()
  val event = AssetProcessingEvent(UUID.randomUUID(), transferId, "matchId-1", SharePoint, Metadata, "s3-source-bucket", "object/key")

  "runCheck" should "return an asset processing error when a file has a size of 0 bytes" in {
    val input = ClientSideMetadataInput("folder1/folder2/file.txt", "checksum", 12L, 0, "matchId-1")
    val check = FileSizeCheck.apply()
    val result = check.runCheck(event, input)
    result.size shouldBe 1
    val error = result.head
    error.matchId.get shouldBe "matchId-1"
    error.source.get shouldBe SharePoint.id
    error.consignmentId.get shouldBe transferId
    error.errorCode shouldBe "INITIAL_CHECKS.OBJECT_SIZE.TOO_SMALL"
    error.errorMsg shouldBe "File size: 0 bytes"
  }

  "runCheck" should "return an asset processing error when a file exceeds the maximum permitted file size" in {
    val input = ClientSideMetadataInput("folder1/folder2/file.txt", "checksum", 12L, 3000L * 1000000L, "matchId-1")
    val check = FileSizeCheck.apply()
    val result = check.runCheck(event, input)
    result.size shouldBe 1
    val error = result.head
    error.matchId.get shouldBe "matchId-1"
    error.source.get shouldBe SharePoint.id
    error.consignmentId.get shouldBe transferId
    error.errorCode shouldBe "INITIAL_CHECKS.OBJECT_SIZE.TOO_BIG"
    error.errorMsg shouldBe "File size: 3000000000 bytes"
  }

  "runCheck" should "not return any error if file size is neither too big, or too small" in {
    val input = ClientSideMetadataInput("folder1/folder2/file.txt", "checksum", 12L, 1000000L, "matchId-1")
    val check = FileSizeCheck.apply()
    val result = check.runCheck(event, input)
    result.size shouldBe 0
  }
}
