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
    val expectedValues = List("AGGREGATE_PROCESSING", "ASSET_PROCESSING", "ORCHESTRATION", "PERSISTENCE")

    processType.values.size shouldBe 4
    processType.values.map(_.toString).toList shouldEqual expectedValues
  }

  "ProcessErrorType" should "contain the correct enums" in {
    val processErrorType = Common.ProcessErrorType
    val expectedValues = List("CLIENT_DATA_LOAD", "ENCODING", "EVENT", "JSON", "MATCH_ID", "OBJECT_KEY", "S3")

    processErrorType.values.size shouldBe 7
    processErrorType.values.map(_.toString).toList shouldEqual expectedValues
  }

  "ProcessErrorValue" should "contain the correct enums" in {
    val processErrorValue = Common.ProcessErrorValue
    val expectedValues = List("FAILURE", "INVALID", "MISMATCH", "READ_ERROR")

    processErrorValue.values.size shouldBe 4
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
    val expectedValues = List("harddrive", "sharepoint")

    assetSourceValue.values.size shouldBe 2
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
    val expectedValues = List("metadata", "records")

    objectCategoryValue.values.size shouldBe 2
    objectCategoryValue.values.map(_.toString).toList shouldEqual expectedValues
  }

  "objectKeyParser" should "parse a valid object key" in {
    val userId = UUID.randomUUID()
    val consignmentId = UUID.randomUUID()
    val matchId = UUID.randomUUID().toString
    val validObjectKey = s"$userId/sharepoint/$consignmentId/metadata/$matchId.metadata"

    val result = Common.objectKeyParser(validObjectKey)
    result.userId shouldBe userId
    result.assetSource shouldBe Common.AssetSource.SharePoint
    result.consignmentId shouldBe consignmentId
    result.category shouldBe Common.ObjectCategory.Metadata
    result.objectElements.get shouldBe s"$matchId.metadata"
  }

  "objectKeyParser" should "parse a valid object prefix key" in {
    val userId = UUID.randomUUID()
    val consignmentId = UUID.randomUUID()
    val validObjectPrefixKey = s"$userId/sharepoint/$consignmentId/metadata"

    val result = Common.objectKeyParser(validObjectPrefixKey)
    result.userId shouldBe userId
    result.assetSource shouldBe Common.AssetSource.SharePoint
    result.consignmentId shouldBe consignmentId
    result.category shouldBe Common.ObjectCategory.Metadata
    result.objectElements shouldBe None
  }

  "objectKeyParser" should "throw a relevant exception when parsing an invalid object key" in {
    val userId = UUID.randomUUID()
    val consignmentId = UUID.randomUUID()
    val invalidObjectKey = s"$userId/$consignmentId/metadata"

    val exception = intercept[NoSuchElementException] {
      Common.objectKeyParser(invalidObjectKey)
    }

    exception.getMessage shouldBe s"No value found for '$consignmentId'"
  }
}
