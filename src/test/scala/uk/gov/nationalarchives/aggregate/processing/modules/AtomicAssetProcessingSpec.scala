package uk.gov.nationalarchives.aggregate.processing.modules

import com.typesafe.scalalogging.Logger
import io.circe.syntax.EncoderOps
import org.mockito.MockitoSugar.mock
import org.slf4j.{Logger => UnderlyingLogger}
import uk.gov.nationalarchives.aggregate.processing.ExternalServiceSpec
import uk.gov.nationalarchives.aggregate.processing.modules.AssetProcessing.RequiredSharePointMetadata
import uk.gov.nationalarchives.aggregate.processing.modules.AtomicAssetProcessor.AtomicAssetProcessingEvent
import uk.gov.nationalarchives.aggregate.processing.modules.Common.AssetSource
import uk.gov.nationalarchives.aggregate.processing.modules.persistence.{DataPersistence, StateCache}

import java.util.UUID
import scala.collection.parallel.CollectionConverters._

class AtomicAssetProcessingSpec extends ExternalServiceSpec {
  private val stateCache = StateCache.apply()

  "process" should "handle a large number of parallel requests using cache" in {
    val mockLogger = mock[UnderlyingLogger]
    val userId = UUID.randomUUID()
    val cid1 = UUID.fromString("7e0ed6b7-82a3-4480-aa2d-826f6133e6f0")
    val cid2 = UUID.fromString("f0df28af-c141-4ad2-b3f2-3e0f4265468b")
    //Tried up to 500000 - success
    val numberOfEvents = 3
    //Seed the reference pool otherwise get duplicated allocation
    stateCache.replenishReferencePool(10)

    List(cid2, cid1).flatMap(cid => {
      (1 to numberOfEvents).map {i =>
        val matchId = UUID.randomUUID()
        val fileName = s"file$i.txt"
        val filePath = s"/sites/Retail/Shared Documents/$fileName"
        val sharePointMetadata = RequiredSharePointMetadata(
          matchId.toString, cid, "2025-07-03T09:19:47Z", "1b47903dfdf5f21abeb7b304efb8e801656bff31225f522406f45c21a68eddf2",
          12L, filePath, fileName).asJson
        AtomicAssetProcessingEvent(AssetSource.SharePoint, cid, matchId.toString, userId, sharePointMetadata)
      }.toList
    }).par.foreach(new AtomicAssetProcessor(StateCache.apply(), DataPersistence.apply())(Logger(mockLogger)).process(_))
  }
}
