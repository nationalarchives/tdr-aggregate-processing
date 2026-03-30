package uk.gov.nationalarchives.aggregate.processing.modules

object Common {
  object ProcessType extends Enumeration {
    type ProcessType = Value
    val AggregateProcessing: Value = Value("AGGREGATE_PROCESSING")
    val AssetProcessing: Value = Value("ASSET_PROCESSING")
    val InitialChecks: Value = Value("INITIAL_CHECKS")
    val Orchestration: Value = Value("ORCHESTRATION")
  }

  object ProcessErrorType extends Enumeration {
    type ProcessErrorType = Value
    val ClientDataLoadError: Value = Value("CLIENT_DATA_LOAD")
    val EncodingError: Value = Value("ENCODING")
    val EventError: Value = Value("EVENT")
    val JsonError: Value = Value("JSON")
    val MalwareScanError: Value = Value("MALWARE_SCAN")
    val MatchIdError: Value = Value("MATCH_ID")
    val ObjectKeyError: Value = Value("OBJECT_KEY")
    val ObjectSizeError: Value = Value("OBJECT_SIZE")
    val S3Error: Value = Value("S3")
    val UploadError: Value = Value("UPLOAD")
  }

  object ProcessErrorValue extends Enumeration {
    type ProcessErrorValue = Value
    val Failure: Value = Value("FAILURE")
    val FolderOnly: Value = Value("FOLDER_ONLY")
    val Invalid: Value = Value("INVALID")
    val Mismatch: Value = Value("MISMATCH")
    val ReadError: Value = Value("READ_ERROR")
    val TooBigError: Value = Value("TOO_BIG")
    val TooSmallError: Value = Value("TOO_SMALL")
    val ThreatFound: Value = Value("THREAT_FOUND")
  }

  object MetadataClassification extends Enumeration {
    type MetadataClassification = Value
    val Supplied, System, Custom = Value
  }

  object TransferFunction extends Enumeration {
    type TransferFunction = Value
    val Load: Value = Value("load")
  }
}
