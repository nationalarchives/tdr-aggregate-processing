package uk.gov.nationalarchives.aggregate.processing.modules.initialchecks

import graphql.codegen.types.ClientSideMetadataInput
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import uk.gov.nationalarchives.aggregate.processing.ExternalServiceSpec
import uk.gov.nationalarchives.aggregate.processing.modules.AssetProcessing.AssetProcessingEvent
import uk.gov.nationalarchives.aggregate.processing.modules.Common.{AssetSource, ObjectType}

import java.util.UUID

class FileExtensionCheckSpec extends ExternalServiceSpec {
  val transferId: UUID = UUID.randomUUID()
  val event = AssetProcessingEvent(UUID.randomUUID(), transferId, "matchId-1", AssetSource.SharePoint, ObjectType.Metadata, "s3-source-bucket", "object/key")

  "runCheck" should "not return any errors" in {
    val input = ClientSideMetadataInput("folder1/folder2/file.txt", "checksum", 12L, 1000000L, "matchId-1")
    val check = FileExtensionCheck.apply()
    val result = check.runCheck(event, input)
    result.size shouldBe 0
  }

}
