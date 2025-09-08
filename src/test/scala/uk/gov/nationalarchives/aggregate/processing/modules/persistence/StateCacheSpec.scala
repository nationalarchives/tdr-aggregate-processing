package uk.gov.nationalarchives.aggregate.processing.modules.persistence

import com.google.gson.Gson
import com.google.gson.internal.LinkedTreeMap
import io.circe.{Json, parser}
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import uk.gov.nationalarchives.aggregate.processing.ExternalServiceSpec
import uk.gov.nationalarchives.aggregate.processing.modules.persistence.Model._

import java.util.UUID

class StateCacheSpec extends  ExternalServiceSpec {
  private def defaultJsonString(matchId: String, consignmentId: UUID) = s"""{
      "Length": "12",
      "Modified": "2025-07-03T09:19:47Z",
      "FileLeafRef": "file1.txt",
      "FileRef": "/sites/Retail/Shared Documents/file1.txt",
      "SHA256ClientSideChecksum": "1b47903dfdf5f21abeb7b304efb8e801656bff31225f522406f45c21a68eddf2",
      "matchId": "$matchId",
      "transferId": "$consignmentId"
    }""".stripMargin

  "x" should "y" in {
    val cache = new StateCache
    val consignmentId = UUID.randomUUID()
    val errorState1 = ErrorState(consignmentId, Model.TransferProcess.AssetProcessing, "match-id-123")
    val errorState2 = ErrorState(consignmentId, Model.TransferProcess.AssetProcessing, "match-id-abc")

    cache.setErrorState(errorState1)
    cache.setErrorState(errorState2)

    val errorState = cache.getErrorState(ErrorStateFilter(consignmentId, Set()))
    errorState.size shouldBe 1
    errorState.keys.head shouldBe TransferProcess.AssetProcessing
    errorState(TransferProcess.AssetProcessing) shouldBe 2
  }
}
