package uk.gov.nationalarchives.aggregate.processing.modules

object Common {
  object ProcessType extends Enumeration {
    type ProcessType = Value
    val AssetProcessing: Value = Value("ASSET_PROCESSING")
  }

  object ProcessErrorType extends Enumeration {
    type ProcessErrorType = Value
    val EncodingError: Value = Value("ENCODING")
    val JsonError: Value = Value("JSON")
    val MetadataFieldError: Value = Value("METADATA_FIELD")
    val ObjectKeyParsingError: Value = Value("OBJECT_KEY")
    val S3Error: Value = Value("S3")
  }

  object ProcessErrorValue extends Enumeration {
    type ProcessErrorValue = Value
    val Invalid: Value = Value("INVALID")
    val Missing: Value = Value("MISSING")
    val ReadError: Value = Value("READ_ERROR")
  }
}
