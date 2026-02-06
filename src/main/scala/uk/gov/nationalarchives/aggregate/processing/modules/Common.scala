package uk.gov.nationalarchives.aggregate.processing.modules

import uk.gov.nationalarchives.aggregate.processing.modules.Common.AssetSource.AssetSource
import uk.gov.nationalarchives.aggregate.processing.modules.Common.ObjectCategory.ObjectCategory

import java.util.UUID
import scala.util.{Failure, Success, Try}

object Common {
  object ConsignmentStatusType extends Enumeration {
    type ConsignmentStatusType = Value
    val Upload: Value = Value("Upload")
    val ClientChecks: Value = Value("ClientChecks")
    val DraftMetadata: Value = Value("DraftMetadata")
  }

  object ProcessType extends Enumeration {
    type ProcessType = Value
    val AggregateProcessing: Value = Value("AGGREGATE_PROCESSING")
    val AssetProcessing: Value = Value("ASSET_PROCESSING")
    val InitialChecks: Value = Value("INITIAL_CHECKS")
    val Persistence: Value = Value("PERSISTENCE")
    val Orchestration: Value = Value("ORCHESTRATION")
  }

  object ProcessErrorType extends Enumeration {
    type ProcessErrorType = Value
    val ClientDataLoadError: Value = Value("CLIENT_DATA_LOAD")
    val EncodingError: Value = Value("ENCODING")
    val EventError: Value = Value("EVENT")
    val FileExtensionError: Value = Value("FILE_EXTENSION")
    val JsonError: Value = Value("JSON")
    val MatchIdError: Value = Value("MATCH_ID")
    val ObjectKeyError: Value = Value("OBJECT_KEY")
    val ObjectSizeError: Value = Value("OBJECT_SIZE")
    val S3Error: Value = Value("S3")
    val MalwareScanError: Value = Value("MALWARE_SCAN")
  }

  object ProcessErrorValue extends Enumeration {
    type ProcessErrorValue = Value
    val Disallowed: Value = Value("DISALLOWED")
    val Failure: Value = Value("FAILURE")
    val Invalid: Value = Value("INVALID")
    val Mismatch: Value = Value("MISMATCH")
    val ReadError: Value = Value("READ_ERROR")
    val TooBigError: Value = Value("TOO_BIG")
    val TooSmallError: Value = Value("TOO_SMALL")
    val ThreatFound: Value = Value("THREAT_FOUND")
  }

  object StateStatusValue extends Enumeration {
    type ConsignmentStatusValue = Value
    val Completed: Value = Value("Completed")
    val CompletedWithIssues: Value = Value("CompletedWithIssues")
    val Failed: Value = Value("Failed")
    val InProgress: Value = Value("InProgress")
  }

  object AssetSource extends Enumeration {
    type AssetSource = Value
    val HardDrive: Value = Value("harddrive")
    val NetworkDrive: Value = Value("networkdrive")
    val SharePoint: Value = Value("sharepoint")
  }

  object ObjectType extends Enumeration {
    type ObjectType = Value
    val Metadata: Value = Value("metadata")
  }

  object ObjectCategory extends Enumeration {
    type ObjectCategory = Value
    val DryRunMetadata: Value = Value("dryrunmetadata")
    val Metadata: Value = Value("metadata")
    val Records: Value = Value("records")
  }

  object MetadataClassification extends Enumeration {
    type MetadataClassification = Value
    val Supplied, System, Custom = Value
  }

  object TransferFunction extends Enumeration {
    type TransferFunction = Value
    val Load: Value = Value("load")
  }

  def objectKeyContextParser(objectKey: String): ObjectKeyContext = {
    Try {
      val elements = objectKey.split('/')
      val userId = UUID.fromString(elements(0))
      val assetSource = AssetSource.withName(elements(1))
      val consignmentId = UUID.fromString(elements(2))
      val objectCategory = ObjectCategory.withName(elements(3))
      val objectElements = if (elements.length > 4) Some(elements.last) else None
      ObjectKeyContext(userId, consignmentId, assetSource, objectCategory, objectElements)
    } match {
      case Failure(ex)               => throw ex
      case Success(objectKeyContext) => objectKeyContext
    }
  }

  case class ObjectKeyContext(userId: UUID, consignmentId: UUID, assetSource: AssetSource, category: ObjectCategory, objectElements: Option[String])
}
