package uk.gov.nationalarchives.aggregate.processing.modules.assetprocessing.initialchecks

import graphql.codegen.types.ClientSideMetadataInput
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import uk.gov.nationalarchives.aggregate.processing.ExternalServiceSpec
import uk.gov.nationalarchives.aggregate.processing.modules.assetprocessing.AssetProcessing.AssetProcessingEvent
import uk.gov.nationalarchives.tdr.common.utils.objectkeycontext.AssetSources.SharePoint
import uk.gov.nationalarchives.tdr.common.utils.objectkeycontext.ObjectTypes.Metadata
import uk.gov.nationalarchives.tdr.schema.generated.ExcludedFilenames

import java.util.UUID

class InvalidFileNameSpec extends ExternalServiceSpec {
  val transferId: UUID = UUID.randomUUID()
  val event = AssetProcessingEvent(UUID.randomUUID(), transferId, "matchId-1", SharePoint, Metadata, "s3-source-bucket", "object/key")

  "runCheck" should "return no asset processing errors for a valid file name" in {
    val input = ClientSideMetadataInput("folder1/folder2/file.txt", "checksum", 12L, 0, "matchId-1")
    val check = InvalidFileName.apply()
    val result = check.runCheck(event, input)
    result.size shouldBe 0
  }

  "runCheck" should "return an asset processing error for invalid file names" in {
    val check = InvalidFileName.apply()
    ExcludedFilenames.all.foreach(e => {
      val invalidFileName = e.pattern
      val input = ClientSideMetadataInput(s"folder1/folder2/$invalidFileName", "checksum", 12L, 0, "matchId-1")
      val result = check.runCheck(event, input)
      result.size shouldBe 1
      result.head.errorCode shouldBe "INITIAL_CHECKS.FILE_NAME.INVALID"
      result.head.errorMsg shouldBe s"Invalid file name: $invalidFileName"
      result.head.matchId.get shouldBe "matchId-1"
      result.head.source.get shouldBe event.source.id
      result.head.consignmentId.get shouldBe event.consignmentId
    })
  }
}
