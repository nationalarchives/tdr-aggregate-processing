package uk.gov.nationalarchives.aggregate.processing.modules

import com.typesafe.scalalogging.Logger
import io.circe.Json
import uk.gov.nationalarchives.aggregate.processing.modules.AssetProcessing.RequiredSharePointMetadata
import uk.gov.nationalarchives.aggregate.processing.modules.AtomicAssetProcessor.{AtomicAssetProcessingEvent, PotentialNode}
import uk.gov.nationalarchives.aggregate.processing.modules.Common.AssetSource.AssetSource
import uk.gov.nationalarchives.aggregate.processing.modules.persistence.Model.AssetState
import uk.gov.nationalarchives.aggregate.processing.modules.persistence.Model.TransferStateCategory.pathsState
import uk.gov.nationalarchives.aggregate.processing.modules.persistence.StateCache

import java.io.{File => JIOFile}
import java.util.UUID
import scala.annotation.tailrec

class AtomicAssetProcessor(stateCache: StateCache)(implicit logger: Logger) {
  def process(event: AtomicAssetProcessingEvent) = {
    val assetId = UUID.randomUUID()
    val fileIds = Set(UUID.randomUUID())
    val originalFilePath = event.assetMetadata.FileRef
    val potentialNodes = generateHierarchyNodes(originalFilePath)
    val stateResults = potentialNodes.map(n => {
      val nodeType = n._2.nodeType
      val assetState = AssetState(event.consignmentId, event.matchId.toString, n._2.path, n._2.nodeId, Set())
      val stateResult = stateCache.createAssetStates(assetState)
      stateResult.map(r => {
        if (nodeType == "File") {
          val parentPath = n._2.parentPath.get

        }
      })
    }).toList

  }

  private def generateHierarchyNodes(path: String): Map[String, PotentialNode] = {
    @tailrec
    def innerFunction(originalPath: String, typeIdentifier: String, nodes: Map[String, PotentialNode]): Map[String, PotentialNode] = {
      val jioFile = new JIOFile(originalPath)
      val parentPath = Option(jioFile.getParent)
      val name = jioFile.getName
      val node = PotentialNode(UUID.randomUUID(), originalPath, typeIdentifier, parentPath)
      val nextMap = nodes + (originalPath -> node)
      if (parentPath.isEmpty) {
        nextMap
      } else {
        innerFunction(parentPath.get, "Folder", nextMap)
      }
    }

    val pathWithoutInitialSlash: String = if (path.startsWith("/")) path.tail else path
    innerFunction(pathWithoutInitialSlash, "File", Map())
  }
}

object AtomicAssetProcessor {
  val logger: Logger = Logger[AssetProcessing]
  val stateCache: StateCache = StateCache.apply()

  case class AtomicAssetProcessingEvent(source: AssetSource, consignmentId: UUID, matchId: UUID, assetMetadata: RequiredSharePointMetadata)
  case class PotentialNode(nodeId: UUID, path: String, nodeType: String, parentPath: Option[String])

  def apply() = new AtomicAssetProcessor(stateCache)(logger)
}
