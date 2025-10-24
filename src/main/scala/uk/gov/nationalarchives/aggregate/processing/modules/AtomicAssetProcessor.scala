package uk.gov.nationalarchives.aggregate.processing.modules

import com.typesafe.scalalogging.Logger
import io.circe.generic.semiauto.deriveEncoder
import io.circe.syntax.EncoderOps
import io.circe.{Encoder, Json, JsonObject}
import uk.gov.nationalarchives.aggregate.processing.modules.AssetProcessing.AssetProcessingError
import uk.gov.nationalarchives.aggregate.processing.modules.AtomicAssetProcessor.{AtomicAssetProcessingEvent, TreeNode, schemaConfig}
import uk.gov.nationalarchives.aggregate.processing.modules.Common.AssetSource.AssetSource
import uk.gov.nationalarchives.aggregate.processing.modules.Common.ProcessType
import uk.gov.nationalarchives.aggregate.processing.modules.ErrorHandling.handleError
import uk.gov.nationalarchives.aggregate.processing.modules.persistence.Model.DataCategory.{assetMetadata, filePathErrorData}
import uk.gov.nationalarchives.aggregate.processing.modules.persistence.Model._
import uk.gov.nationalarchives.aggregate.processing.modules.persistence.{DataPersistence, MetadataUtils, StateCache}
import uk.gov.nationalarchives.tdr.schemautils.ConfigUtils

import java.io.{File => JIOFile}
import java.util.UUID
import scala.annotation.tailrec

class AtomicAssetProcessor(stateCache: StateCache, dataPersistence: DataPersistence)(implicit logger: Logger) {
  implicit val errorEncoder: Encoder[AssetProcessingError] = deriveEncoder[AssetProcessingError]

  private val sharePointMapper = schemaConfig.inputToPropertyMapper("sharePointTag")
  private val defaultPropertyValues = schemaConfig.getPropertiesWithDefaultValue

  private def convertToBaseMetadata(sourceJson: Json) = {
    sourceJson.asObject.get.toMap.map(fv => {
      val originalField = fv._1
      val field = sharePointMapper(originalField)
      field -> fv._2
    }).asJson
  }

  private def createFolderAsset(node: TreeNode): JsonObject = {
    addNodeDetails(node, None, JsonObject.apply(
      ("file_path", s"${node.path}".asJson)
    ))
  }

  private def addNodeDetails(node: TreeNode, fileId: Option[UUID], sourceJson: JsonObject): JsonObject = {
    val result = sourceJson
      .add("UUID", s"${node.assetId}".asJson)
      .add("file_type", s"${node.nodeType}".asJson)
      .add("file_reference", s"${node.fileReference}".asJson)
    if (fileId.nonEmpty) {
      result.add("file_ids", s"[${fileId.get}]".asJson)
    } else result
  }

  private def addDefaultMetadata(node: TreeNode, fileId: Some[UUID], sourceJson: JsonObject): JsonObject = {
    //Should only add this if the default field is not present in the source json
    val interimJson = addNodeDetails(node, fileId, sourceJson)
    val e = defaultPropertyValues.map(kv => {
      (kv._1, kv._2.asJson)
    })
    (interimJson.toMap ++ e).asJson.asObject.get
  }

  private def addParentReference(consignmentId: UUID, node: TreeNode, sourceJson: JsonObject): JsonObject = {
    if (node.hasParent) {
      val parentReference: String = stateCache.getReferenceByPath(consignmentId, node.parentPath.get)
      sourceJson.add("parent_reference", s"$parentReference".asJson)
    } else {
      sourceJson
    }
  }

  def process(event: AtomicAssetProcessingEvent): Unit = {
    val consignmentId = event.consignmentId
    val matchId = event.matchId
    val objectState = TransferState(consignmentId, TransferStateCategory.uploadedObjectsState, matchId)
    val objectStateResult = stateCache.updateTransferState(objectState)
    if (objectStateResult == 0) {
      val error = AssetProcessingError(Some(consignmentId.toString), Some(matchId), Some(event.source.toString),
        s"${ProcessType.AssetProcessing}.MatchId.Duplicate", s"Duplicate match id found for consignment: $consignmentId")
      handleError(error, logger)
      dataPersistence.setErrorData(ErrorData(consignmentId, matchId, DataCategory.matchIdErrorData, error.asJson))
    } else {
      createAssets(event)
    }
  }

  private def createAssets(event: AtomicAssetProcessingEvent): Unit = {
    val consignmentId = event.consignmentId
    val matchId = event.matchId
    val jsonMetadata = convertToBaseMetadata(event.assetMetadata).asObject.get
    val originalFilePath = jsonMetadata("file_path").get.asString.get
    val treeNodes = generateTreeNodes(matchId, consignmentId, originalFilePath)
    treeNodes.foreach(treeNode => {
      val assetId = treeNode.assetId
      val pathToAssetIdState = PathToAssetIdState(consignmentId, treeNode.path, assetId.toString)
      stateCache.updatePathToAssetState(pathToAssetIdState)
      val assetJsonObject: JsonObject = if (treeNode.nodeType == "File") {
        val fileId = UUID.randomUUID()
        val matchIdToFileIdState = MatchIdToFileIdState(consignmentId, matchId, fileId)
        stateCache.updateMatchIdToFileIdState(matchIdToFileIdState)
        addDefaultMetadata(treeNode, Some(fileId), jsonMetadata)
      } else {
        createFolderAsset(treeNode)
      }

      val enrichedJson = addParentReference(consignmentId, treeNode, assetJsonObject).asJson
      val assetData = AssetData(consignmentId, assetId, matchId, event.userId, assetMetadata, enrichedJson)
      dataPersistence.setAssetData(assetData)
//      new MetadataUtils().insertAssetData(List(assetData))
    })
  }

  private def generateTreeNodes(matchId: String, consignmentId: UUID, path: String): List[TreeNode] = {
    @tailrec
    def innerFunction(originalPath: String, typeIdentifier: String, nodes: List[TreeNode]): List[TreeNode] = {
      val jioFile = new JIOFile(originalPath)
      val parentPath = Option(jioFile.getParent)
      val stateResult = stateCache.updateTransferState(TransferState(consignmentId, TransferStateCategory.pathsState, originalPath))
      stateResult match {
        case 0 if typeIdentifier == "File" =>
          val error = AssetProcessingError(
            Some(consignmentId.toString), Some(matchId), Some("sharePoint"), "", s"Duplicate file path in consignment: $originalPath")
          handleError(error, logger)
          dataPersistence.setErrorData(ErrorData(consignmentId, matchId, filePathErrorData, error.asJson))
          nodes
        case 0 => nodes
        case _ =>
          val nodeId = UUID.randomUUID()
          val reference = stateCache.getReferences(1).head
          val pathToAssetIdState = PathToAssetIdState(consignmentId, originalPath, nodeId.toString)
          stateCache.updatePathToAssetState(pathToAssetIdState)
          val pathToReferenceState = PathToReferenceState(consignmentId, originalPath, reference)
          updateReferenceState(matchId, pathToReferenceState)
          val node = TreeNode(nodeId, originalPath, typeIdentifier, parentPath, reference, parentPath.nonEmpty)
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

  private def updateReferenceState(matchId: String, state: PathToReferenceState): Unit = {
    val result = stateCache.updateAssignedReferences(state.consignmentId, state.reference)
    if (result == 0) {

      val error = AssetProcessingError(Some(state.consignmentId.toString), Some(matchId), Some(DataCategory.referenceErrorData.toString),
        s"${ProcessType.AssetProcessing}.Reference.Duplicate", s"Duplicate reference used for consignment: ${state.consignmentId}")
      handleError(error, logger)
      dataPersistence.setErrorData(ErrorData(state.consignmentId, matchId, DataCategory.referenceErrorData, error.asJson))
    } else stateCache.updatePathToReferenceState(state)
  }
}

object AtomicAssetProcessor {
  val logger: Logger = Logger[AssetProcessing]
  private val stateCache: StateCache = StateCache.apply()
  private val dataPersistence = DataPersistence.apply()
  private val schemaConfig = ConfigUtils.loadConfiguration

  case class AtomicAssetProcessingEvent(source: AssetSource, consignmentId: UUID, matchId: String, userId: UUID, assetMetadata: Json)
  case class TreeNode(assetId: UUID, path: String, nodeType: String, parentPath: Option[String], fileReference: String, hasParent: Boolean)

  def apply() = new AtomicAssetProcessor(stateCache, dataPersistence)(logger)
}
