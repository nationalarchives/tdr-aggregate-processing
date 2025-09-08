package uk.gov.nationalarchives.aggregate.processing.modules.persistence

import io.circe.Json
import uk.gov.nationalarchives.aggregate.processing.modules.persistence.Model.TransferProcess.TransferProcess
import uk.gov.nationalarchives.aggregate.processing.modules.persistence.Model.DataCategory
import uk.gov.nationalarchives.aggregate.processing.modules.persistence.Model.DataCategory.AssetDataCategory

import java.util.UUID

object Model {
  object TransferProcess extends Enumeration {
    type TransferProcess = Value
    val AssetProcessing: Value = Value("asset")
  }

  object TransferStateCategory extends Enumeration {
    type TransferStateCategory = Value
    val errorState: Value = Value("errors")
    val pathsState: Value = Value("paths")
  }

  object DataCategory extends Enumeration {
    type AssetDataCategory = Value
    type ConsignmentDataCategory = Value
    val errorData: Value = Value("error")
  }

  trait State { }

  case class ErrorState(consignmentId: UUID, transferProcess: TransferProcess, matchId: String) extends State
  case class PathState(consignmentId: UUID, path: String) extends State

  trait StateFilter { }

  case class ErrorStateFilter(consignmentId: UUID, filter: Set[TransferProcess]) extends StateFilter

  trait JsonData { }

  case class AssetData(matchId: String, assetId: Option[UUID], category: AssetDataCategory, input: Json) extends JsonData
}
