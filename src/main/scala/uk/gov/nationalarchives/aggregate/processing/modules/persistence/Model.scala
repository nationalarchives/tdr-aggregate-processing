package uk.gov.nationalarchives.aggregate.processing.modules.persistence

import io.circe.Json
import redis.clients.jedis.json.Path2
import uk.gov.nationalarchives.aggregate.processing.modules.persistence.Model.DataCategory.DataCategory
import uk.gov.nationalarchives.aggregate.processing.modules.persistence.Model.TransferProcess.TransferProcess
import uk.gov.nationalarchives.aggregate.processing.modules.persistence.Model.TransferStateCategory.TransferStateCategory

import java.util.UUID

object Model {
  trait Filter extends Enumeration { }

  object TransferProcess extends Filter {
    type TransferProcess = Value
    val AssetProcessing: Value = Value("asset")
  }

  object TransferStateCategory extends Filter {
    type TransferStateCategory = Value
    val errorState: Value = Value("errors")
    val matchIdToFileIdState: Value = Value("matchIdToFileId")
    val pathsState: Value = Value("paths")
    val uploadedObjectsState: Value = Value("uploadedObjects")
    val pathToAssetState: Value = Value("pathToAsset")
    val pathToReferenceState: Value = Value("pathToReference")
  }

  object DataCategory extends Filter {
    type DataCategory = Value
    val assetMetadata: Value = Value("assetMetadata")
    val filePathErrorData: Value = Value("filePath")
    val matchIdErrorData: Value = Value("matchId")
    val referenceErrorData: Value = Value("reference")
  }

  trait State { }
  case class PathToAssetIdState(consignmentId: UUID, path: String, assetIdentifier: String) extends State
  case class PathToReferenceState(consignmentId: UUID, path: String, reference: String) extends State
  case class MatchIdToFileIdState(consignmentId: UUID, matchId: String, fileId: UUID) extends State
  case class TransferState(consignmentId: UUID, transferState: TransferStateCategory, value: String) extends State

  trait StateFilter { }
  case class ErrorStateFilter(consignmentId: UUID, filter: Set[TransferProcess]) extends StateFilter
  case class TransferStateFilter(consignmentId: UUID, filter: Set[TransferStateCategory]) extends StateFilter

  trait JsonData { }
  case class AssetData(consignmentId: UUID, assetId: UUID, category: DataCategory, input: Json, pathTo: Option[Path2] = None) extends JsonData
  case class ErrorData(consignmentId: UUID, objectIdentifier: String, DataCategory: DataCategory, input: Json, pathTo: Option[Path2] = None)
}
