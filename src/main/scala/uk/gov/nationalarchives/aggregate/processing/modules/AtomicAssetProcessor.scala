package uk.gov.nationalarchives.aggregate.processing.modules

import com.typesafe.scalalogging.Logger
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.syntax.EncoderOps
import io.circe.{Decoder, Encoder, Json, JsonObject}
import uk.gov.nationalarchives.aggregate.processing.modules.AssetProcessing.{AssetProcessingError, RequiredSharePointMetadata}
import uk.gov.nationalarchives.aggregate.processing.modules.AtomicAssetProcessor.{AtomicAssetProcessingEvent, PotentialNode}
import uk.gov.nationalarchives.aggregate.processing.modules.Common.AssetSource.AssetSource
import uk.gov.nationalarchives.aggregate.processing.modules.Common.ProcessType
import uk.gov.nationalarchives.aggregate.processing.modules.ErrorHandling.handleError
import uk.gov.nationalarchives.aggregate.processing.modules.persistence.Model.DataCategory.{assetMetadata, filePathErrorData}
import uk.gov.nationalarchives.aggregate.processing.modules.persistence.Model.{AssetData, DataCategory, ErrorData, MatchIdToFileIdState, PathToAssetIdState, TransferState, TransferStateCategory}
import uk.gov.nationalarchives.aggregate.processing.modules.persistence.{DataPersistence, StateCache}

import java.io.{File => JIOFile}
import java.util.UUID
import scala.annotation.tailrec

class AtomicAssetProcessor(stateCache: StateCache, dataPersistence: DataPersistence)(implicit logger: Logger) {
  implicit val clientSideInputDecoder: Decoder[RequiredSharePointMetadata] = deriveDecoder[RequiredSharePointMetadata]
  implicit val clientSideInputEncoder: Encoder[RequiredSharePointMetadata] = deriveEncoder[RequiredSharePointMetadata]
  implicit val errorDecoder: Decoder[AssetProcessingError] = deriveDecoder[AssetProcessingError]
  implicit val errorEncoder: Encoder[AssetProcessingError] = deriveEncoder[AssetProcessingError]

  def process(event: AtomicAssetProcessingEvent): Unit = {
    val consignmentId = event.consignmentId
    val matchId = event.matchId
    val objectState = TransferState(consignmentId, TransferStateCategory.objectsState, matchId.toString)
    val objectStateResult = stateCache.createTransferState(objectState)
    if (objectStateResult == 0) {
      val error = AssetProcessingError(Some(consignmentId.toString), Some(matchId.toString), Some(event.source.toString),
        s"${ProcessType.AssetProcessing}.MatchId.Duplicate", s"Duplicate match id found for consignment: $consignmentId")
      handleError(error, logger)
      dataPersistence.setErrorData(ErrorData(consignmentId, matchId.toString, DataCategory.matchIdErrorData, error.asJson))
    } else {
      createAssets(event)
    }
  }

  private def createAssets(event: AtomicAssetProcessingEvent): Unit = {
    val consignmentId = event.consignmentId
    val originalFilePath = event.assetMetadata.FileRef
    val potentialNodes = generatePotentialNodes(event.matchId.toString, consignmentId, originalFilePath)
    potentialNodes.foreach(pn => {
      val nodeType = pn.nodeType
      val pathToAssetIdState = PathToAssetIdState(consignmentId, pn.path, pn.nodeId.toString)
      stateCache.createPathToAssetState(pathToAssetIdState)
      val parentPath = pn.parentPath
      val parentId: Option[String] =
        if (parentPath.isEmpty) { None } else Some(stateCache.getAssetIdentifierByPath(consignmentId, parentPath.get))

      val assetJsonObject: JsonObject = if (nodeType == "File") {
        val fileJsonObject: JsonObject = event.assetMetadata.asJson.asObject.get
        val fileId = UUID.randomUUID()
        val matchIdToFileIdState = MatchIdToFileIdState(consignmentId, event.matchId.toString, fileId)
        stateCache.createMatchIdToFileIdState(matchIdToFileIdState)
        val enrichedObject = fileJsonObject
          .add("uuid", s"${pn.nodeId}".asJson)
          .add("fileIds", s"[$fileId]".asJson)
          .add("FileType", s"${pn.nodeType}".asJson)
        enrichedObject
      } else {
        JsonObject.apply(
          ("uuid", s"${pn.nodeId}".asJson),
          ("FileType", s"${pn.nodeType}".asJson),
          ("FilePath", s"${pn.path}".asJson))
      }

      val enrichedJson = if (parentId.isEmpty) { assetJsonObject.toJson } else
        assetJsonObject.add("ParentId", s"${parentId.get}".asJson).toJson

      dataPersistence.setAssetData(AssetData(consignmentId, pn.nodeId, assetMetadata, enrichedJson))
    })
  }

  private def generatePotentialNodes(matchId: String, consignmentId: UUID, path: String): List[PotentialNode] = {
    @tailrec
    def innerFunction(originalPath: String, typeIdentifier: String, nodes: List[PotentialNode]): List[PotentialNode] = {
      val jioFile = new JIOFile(originalPath)
      val parentPath = Option(jioFile.getParent)
      val stateResult = stateCache.createTransferState(TransferState(consignmentId, TransferStateCategory.pathsState, originalPath))
      stateResult match {
        case 0 if typeIdentifier == "File" =>
          val error = AssetProcessingError(Some(consignmentId.toString), Some(matchId), Some("sharePoint"), "", s"Duplicate file path in consignment: $originalPath")
          handleError(error, logger)
          dataPersistence.setErrorData(ErrorData(consignmentId, matchId, filePathErrorData, error.asJson))
          nodes
        case 0 => nodes
        case _ =>
          val nodeId = UUID.randomUUID()
          val pathToAssetIdState = PathToAssetIdState(consignmentId, originalPath, nodeId.toString)
          stateCache.createPathToAssetState(pathToAssetIdState)
          val node = PotentialNode(nodeId, originalPath, typeIdentifier, parentPath)
          val nextList = nodes :+ node
          if (parentPath.nonEmpty) {
            innerFunction(parentPath.get, "Folder", nextList)
          } else {
            nextList
          }
      }
    }

    val pathWithoutInitialSlash: String = if (path.startsWith("/")) path.tail else path
    innerFunction(pathWithoutInitialSlash, "File", List())
  }
}

object AtomicAssetProcessor {
  val logger: Logger = Logger[AssetProcessing]
  private val stateCache: StateCache = StateCache.apply()
  private val dataPersistence = DataPersistence.apply()

  case class AtomicAssetProcessingEvent(source: AssetSource, consignmentId: UUID, matchId: UUID, assetMetadata: RequiredSharePointMetadata)
  case class PotentialNode(nodeId: UUID, path: String, nodeType: String, parentPath: Option[String])

  def apply() = new AtomicAssetProcessor(stateCache, dataPersistence)(logger)
}
