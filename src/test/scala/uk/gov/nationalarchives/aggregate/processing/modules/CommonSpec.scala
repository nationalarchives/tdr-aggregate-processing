package uk.gov.nationalarchives.aggregate.processing.modules

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class CommonSpec extends AnyFlatSpec {
  "ProcessType" should "contain the correct enums" in {
    val processType = Common.ProcessType
    val expectedValues = List("AGGREGATE_PROCESSING", "ASSET_PROCESSING", "INITIAL_CHECKS", "ORCHESTRATION")

    processType.values.size shouldBe 4
    processType.values.map(_.toString).toList shouldEqual expectedValues
  }

  "ProcessErrorType" should "contain the correct enums" in {
    val processErrorType = Common.ProcessErrorType
    val expectedValues =
      List("CLIENT_DATA_LOAD", "ENCODING", "EVENT", "JSON", "MALWARE_SCAN", "MATCH_ID", "OBJECT_KEY", "OBJECT_SIZE", "S3", "UPLOAD")

    processErrorType.values.size shouldBe 10
    processErrorType.values.map(_.toString).toList shouldEqual expectedValues
  }

  "ProcessErrorValue" should "contain the correct enums" in {
    val processErrorValue = Common.ProcessErrorValue
    val expectedValues = List("FAILURE", "FOLDER_ONLY", "INVALID", "MISMATCH", "READ_ERROR", "THREAT_FOUND", "TOO_BIG", "TOO_SMALL")

    processErrorValue.values.size shouldBe 8
    processErrorValue.values.map(_.toString).toList shouldEqual expectedValues
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
}
