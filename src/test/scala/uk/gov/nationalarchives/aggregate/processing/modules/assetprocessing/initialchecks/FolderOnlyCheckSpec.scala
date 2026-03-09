package uk.gov.nationalarchives.aggregate.processing.modules.assetprocessing.initialchecks

import graphql.codegen.types.ClientSideMetadataInput
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import uk.gov.nationalarchives.aggregate.processing.ExternalServiceSpec
import uk.gov.nationalarchives.aggregate.processing.modules.Common.{AssetSource, ObjectType}
import uk.gov.nationalarchives.aggregate.processing.modules.assetprocessing.AssetProcessing.AssetProcessingEvent

import java.util.UUID

class FolderOnlyCheckSpec extends ExternalServiceSpec {
  val transferId: UUID = UUID.randomUUID()
  val event = AssetProcessingEvent(UUID.randomUUID(), transferId, "matchId-1", AssetSource.SharePoint, ObjectType.Metadata, "s3-source-bucket", "object/key")

  "runCheck" should "not return any errors where an extension is present" in {
    val input = ClientSideMetadataInput("folder1/folder2/file.txt", "checksum", 12L, 1000000L, "matchId-1")
    val check = FolderOnlyCheck.apply()
    val result = check.runCheck(event, input)
    result.size shouldBe 0
  }

  "runCheck" should "return an error where the file path does not contain an extension" in {
    val input = ClientSideMetadataInput("folder1/folder2", "checksum", 12L, 1000000L, "matchId-1")
    val check = FolderOnlyCheck.apply()
    val result = check.runCheck(event, input)
    result.size shouldBe 1
    result.head.matchId.get shouldEqual "matchId-1"
    result.head.source.get shouldEqual "sharepoint"
    result.head.consignmentId.get shouldBe transferId
    result.head.errorMsg shouldEqual "Empty folder uploaded"
    result.head.errorCode shouldEqual "INITIAL_CHECKS.UPLOAD.FOLDER_ONLY"
  }
}
