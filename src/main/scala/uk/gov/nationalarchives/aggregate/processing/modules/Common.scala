package uk.gov.nationalarchives.aggregate.processing.modules

import uk.gov.nationalarchives.aggregate.processing.modules.ConsignmentStatusType.ConsignmentStatusType
import uk.gov.nationalarchives.aggregate.processing.modules.StateStatusValue.{ConsignmentStatusValue, TransferStateStatus}
import uk.gov.nationalarchives.aggregate.processing.modules.TransferStateType.TransferStateType

trait ProcessingError {
  def errorCode: String
}

case class TransferState(stateType: TransferStateType, stateStatus: TransferStateStatus, suppliedMetadata: Boolean = false)
case class ConsignmentStatus(statusType: ConsignmentStatusType, statusValue: ConsignmentStatusValue)

object Source extends Enumeration {
  type Source = Value
  val SharePoint: Value = Value("sharepoint")
}

object TransferStateType extends Enumeration {
  type TransferStateType = Value
  val DataLoad: Value = Value("DataLoad")
}

object ConsignmentStatusType extends Enumeration {
  type ConsignmentStatusType = Value
  val Upload: Value = Value("Upload")
}

object StateStatusValue extends Enumeration {
  type TransferStateStatus = Value
  type ConsignmentStatusValue = Value
  val Completed: Value = Value("Completed")
  val completedWithIssues: Value = Value("CompletedWithIssues")
  val Failed: Value = Value("Failed")
}
