package uk.gov.nationalarchives.aggregate.processing.modules.persistence

import io.circe.Json
import uk.gov.nationalarchives.aggregate.processing.modules.persistence.Model.TransferProcess.TransferProcess
import uk.gov.nationalarchives.aggregate.processing.modules.persistence.Model.DataCategory
import uk.gov.nationalarchives.aggregate.processing.modules.persistence.Model.DataCategory.AssetDataCategory
import uk.gov.nationalarchives.aggregate.processing.modules.persistence.Model.TransferStateCategory.TransferStateCategory

import java.util.UUID

object Model {

  trait Filter extends Enumeration { }
  trait State { }

  object TransferProcess extends Filter {
    type TransferProcess = Value
    val AssetProcessing: Value = Value("asset")
  }

  object TransferStateCategory extends Filter {
    type TransferStateCategory = Value
    val assetState: Value = Value("assets")
    val errorState: Value = Value("errors")
    val fileChecksState: Value = Value("fileChecks")
    val pathsState: Value = Value("paths")
  }

  object DataCategory extends Filter {
    type AssetDataCategory = Value
    type ConsignmentDataCategory = Value
    val errorData: Value = Value("error")
  }

  trait StateValue {}
  case class ErrorState(consignmentId: UUID, transferProcess: TransferProcess, matchId: String) extends State
  case class PathState(consignmentId: UUID, path: String, assetIdentifier: String) extends State
  case class FileChecksState(consignmentId: UUID, fileIdentifier: String) extends State
  case class AssetIdentifier(id: UUID) extends StateValue
  case class TransferState(consignmentId: UUID, transferState: TransferStateCategory, value: StateValue)

  trait StateFilter { }

  case class ErrorStateFilter(consignmentId: UUID, filter: Set[TransferProcess]) extends StateFilter
  case class TransferStateFilter(consignmentId: UUID, filter: Set[TransferStateCategory]) extends StateFilter

  trait JsonData { }

  case class AssetData(matchId: String, assetId: Option[UUID], category: AssetDataCategory, input: Json) extends JsonData
}
