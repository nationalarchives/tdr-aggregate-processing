package uk.gov.nationalarchives.aggregate.processing.modules

object Common {
  object ConsignmentStatusType extends Enumeration {
    type ConsignmentStatusType = Value
    val Upload: Value = Value("Upload")
    val ClientChecks: Value = Value("ClientChecks")
    val DraftMetadata: Value = Value("DraftMetadata")
  }

  object ProcessType extends Enumeration {
    type ProcessType = Value
    val AssetProcessing: Value = Value("ASSET_PROCESSING")
    val DraftMetadataValidation: Value = Value("DRAFT_METADATA_VALIDATION")
    val FileChecks: Value = Value("FILE_CHECKS")
  }

  object ProcessErrorType extends Enumeration {
    type ProcessErrorType = Value
    val JsonError: Value = Value("JSON")
    val MetadataProperty: Value = Value("METADATA_PROPERTY")
    val UtfError: Value = Value("UTF")
  }

  object ProcessErrorValue extends Enumeration {
    type ProcessErrorValue = Value
    val Invalid: Value = Value("INVALID")
    val Missing: Value = Value("MISSING")
  }

  object StateStatusValue extends Enumeration {
    type ConsignmentStatusValue = Value
    val Completed: Value = Value("Completed")
    val CompletedWithIssues: Value = Value("CompletedWithIssues")
    val Failed: Value = Value("Failed")
  }
}
