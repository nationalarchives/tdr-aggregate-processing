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
    val Persistence: Value = Value("PERSISTENCE")
    val Orchestration: Value = Value("ORCHESTRATION")
  }

  object ProcessErrorType extends Enumeration {
    type ProcessErrorType = Value
    val ClientSideMetadataError: Value = Value("CLIENT_SIDE_METADATA")
    val EncodingError: Value = Value("ENCODING")
    val EventError: Value = Value("EVENT")
    val JsonError: Value = Value("JSON")
    val ObjectKeyParsingError: Value = Value("OBJECT_KEY")
    val S3Error: Value = Value("S3")
  }

  object ProcessErrorValue extends Enumeration {
    type ProcessErrorValue = Value
    val Invalid: Value = Value("INVALID")
    val ReadError: Value = Value("READ_ERROR")
  }

  object StateStatusValue extends Enumeration {
    type ConsignmentStatusValue = Value
    val Completed: Value = Value("Completed")
    val CompletedWithIssues: Value = Value("CompletedWithIssues")
    val Failed: Value = Value("Failed")
  }

  object AssetSource extends Enumeration {
    type AssetSource = Value
    val SharePoint: Value = Value("sharepoint")
  }

  object ObjectType extends Enumeration {
    type ObjectType = Value
    val Metadata: Value = Value("metadata")
  }

  object ObjectCategory extends Enumeration {
    type ObjectCategory = Value
    val Metadata: Value = Value("metadata")
  }

  def objectKeyParser(objectKey: String): ObjectKeyDetails = {
    Try {
      val elements = objectKey.split('/')
      val userId = UUID.fromString(elements(0))
      val assetSource = AssetSource.withName(elements(1))
      val consignmentId = UUID.fromString(elements(2))
      val objectCategory = ObjectCategory.withName(elements(3))
      val objectElements = if (elements.length > 4) Some(elements.last) else None
      ObjectKeyDetails(userId, consignmentId, assetSource, objectCategory, objectElements)
    } match {
      case Failure(ex)               => throw ex
      case Success(objectKeyDetails) => objectKeyDetails
    }
  }

  case class ObjectKeyDetails(userId: UUID, consignmentId: UUID, assetSource: AssetSource, category: ObjectCategory, objectElements: Option[String])
}
