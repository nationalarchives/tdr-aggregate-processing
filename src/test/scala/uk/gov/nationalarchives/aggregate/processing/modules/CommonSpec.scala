package uk.gov.nationalarchives.aggregate.processing.modules

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.util.UUID

class CommonSpec extends AnyFlatSpec {
  "ConsignmentStatusType" should "contain the correct enums" in {
    val consignmentStatusType = Common.ConsignmentStatusType
    val expectedValues = List("ClientChecks", "DraftMetadata", "Upload")

    consignmentStatusType.values.size shouldBe 3
    consignmentStatusType.values.map(_.toString).toList shouldEqual expectedValues
  }

  "ProcessType" should "contain the correct enums" in {
    val processType = Common.ProcessType
    val expectedValues = List("AGGREGATE_PROCESSING", "ASSET_PROCESSING", "INITIAL_CHECKS", "ORCHESTRATION", "PERSISTENCE")

    processType.values.size shouldBe 5
    processType.values.map(_.toString).toList shouldEqual expectedValues
  }

  "ProcessErrorType" should "contain the correct enums" in {
    val processErrorType = Common.ProcessErrorType
    val expectedValues = List("CLIENT_DATA_LOAD", "ENCODING", "EVENT", "FILE_EXTENSION", "JSON", "MALWARE_SCAN", "MATCH_ID", "OBJECT_KEY", "OBJECT_SIZE", "S3")

    processErrorType.values.size shouldBe 10
    processErrorType.values.map(_.toString).toList shouldEqual expectedValues
  }

  "ProcessErrorValue" should "contain the correct enums" in {
    val processErrorValue = Common.ProcessErrorValue
    val expectedValues = List("DISALLOWED", "FAILURE", "INVALID", "MISMATCH", "READ_ERROR", "THREAT_FOUND", "TOO_BIG", "TOO_SMALL")

    processErrorValue.values.size shouldBe 8
    processErrorValue.values.map(_.toString).toList shouldEqual expectedValues
  }

  "StateStatusValue" should "contain the correct enums" in {
    val stateStatusValue = Common.StateStatusValue
    val expectedValues = List("Completed", "CompletedWithIssues", "Failed", "InProgress")

    stateStatusValue.values.size shouldBe 4
    stateStatusValue.values.map(_.toString).toList shouldEqual expectedValues
  }

  "AssetSource" should "contain the correct enums" in {
    val assetSourceValue = Common.AssetSource
    val expectedValues = List("harddrive", "networkdrive", "sharepoint")

    assetSourceValue.values.size shouldBe 3
    assetSourceValue.values.map(_.toString).toList shouldEqual expectedValues
  }

  "ObjectType" should "contain the correct enums" in {
    val objectTypeValue = Common.ObjectType
    val expectedValues = List("metadata")

    objectTypeValue.values.size shouldBe 1
    objectTypeValue.values.map(_.toString).toList shouldEqual expectedValues
  }

  "ObjectCategory" should "contain the correct enums" in {
    val objectCategoryValue = Common.ObjectCategory
    val expectedValues = List("dryrunmetadata", "metadata", "records")

    objectCategoryValue.values.size shouldBe 3
    objectCategoryValue.values.map(_.toString).toList shouldEqual expectedValues
  }

  "MetadataClassification" should "contain the correct enums" in {
    val metadataClassificationValue = Common.MetadataClassification
    val expectedValues = List("Custom", "Supplied", "System")

    metadataClassificationValue.values.size shouldBe 3
    metadataClassificationValue.values.map(_.toString).toList shouldEqual expectedValues
  }

  "TransferFunction" should "contain the correct enums" in {
    val transferFunctionValue = Common.TransferFunction
    val expectedValues = List("load")

    transferFunctionValue.values.size shouldBe 1
    transferFunctionValue.values.map(_.toString).toList shouldEqual expectedValues
  }

  "objectKeyContextParser" should "parse a valid object key" in {
    val userId = UUID.randomUUID()
    val consignmentId = UUID.randomUUID()
    val matchId = UUID.randomUUID().toString
    val validObjectKey = s"$userId/sharepoint/$consignmentId/metadata/$matchId.metadata"

    val result = Common.objectKeyContextParser(validObjectKey)
    result.userId shouldBe userId
    result.assetSource shouldBe Common.AssetSource.SharePoint
    result.consignmentId shouldBe consignmentId
    result.category shouldBe Common.ObjectCategory.Metadata
    result.objectElements.get shouldBe s"$matchId.metadata"
  }

  "objectKeyContextParser" should "parse a valid object prefix key" in {
    val userId = UUID.randomUUID()
    val consignmentId = UUID.randomUUID()
    val validObjectPrefixKey = s"$userId/sharepoint/$consignmentId/metadata"

    val result = Common.objectKeyContextParser(validObjectPrefixKey)
    result.userId shouldBe userId
    result.assetSource shouldBe Common.AssetSource.SharePoint
    result.consignmentId shouldBe consignmentId
    result.category shouldBe Common.ObjectCategory.Metadata
    result.objectElements shouldBe None
  }

  "objectKeyContextParser" should "throw a relevant exception when parsing an invalid object key" in {
    val userId = UUID.randomUUID()
    val consignmentId = UUID.randomUUID()
    val invalidObjectKey = s"$userId/$consignmentId/metadata"

    val exception = intercept[NoSuchElementException] {
      Common.objectKeyContextParser(invalidObjectKey)
    }

    exception.getMessage shouldBe s"No value found for '$consignmentId'"
  }
}
