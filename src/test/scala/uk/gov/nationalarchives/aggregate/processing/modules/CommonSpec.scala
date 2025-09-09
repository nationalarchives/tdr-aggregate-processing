package uk.gov.nationalarchives.aggregate.processing.modules

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class CommonSpec extends AnyFlatSpec {
  "ConsignmentStatusType" should "contain the correct enums" in {
    val consignmentStatusType = Common.ConsignmentStatusType
    val expectedValues = List("ClientChecks", "DraftMetadata", "Upload")

    consignmentStatusType.values.size shouldBe 3
    consignmentStatusType.values.map(_.toString).toList shouldEqual expectedValues
  }

  "ProcessType" should "contain the correct enums" in {
    val processType = Common.ProcessType
    val expectedValues = List("ASSET_PROCESSING")

    processType.values.size shouldBe 1
    processType.values.map(_.toString).toList shouldEqual expectedValues
  }

  "ProcessErrorType" should "contain the correct enums" in {
    val processErrorType = Common.ProcessErrorType
    val expectedValues = List("ENCODING", "JSON", "METADATA_FIELD", "OBJECT_KEY", "S3")

    processErrorType.values.size shouldBe 5
    processErrorType.values.map(_.toString).toList shouldEqual expectedValues
  }

  "ProcessErrorValue" should "contain the correct enums" in {
    val processErrorValue = Common.ProcessErrorValue
    val expectedValues = List("INVALID", "MISSING", "READ_ERROR")

    processErrorValue.values.size shouldBe 3
    processErrorValue.values.map(_.toString).toList shouldEqual expectedValues
  }

  "StateStatusValue" should "contain the correct enums" in {
    val stateStatusValue = Common.StateStatusValue
    val expectedValues = List("Completed", "CompletedWithIssues", "Failed")

    stateStatusValue.values.size shouldBe 3
    stateStatusValue.values.map(_.toString).toList shouldEqual expectedValues
  }
}
