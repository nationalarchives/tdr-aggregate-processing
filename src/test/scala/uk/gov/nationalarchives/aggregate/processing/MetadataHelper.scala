package uk.gov.nationalarchives.aggregate.processing

import io.circe.{Json, parser}
import uk.gov.nationalarchives.aggregate.processing.modules.assetprocessing.metadata.MetadataProperty

import java.util.UUID

trait MetadataHelper {
  val defaultFileSize: Long = 12L
  val customFieldJsonString: String = """"customField": "custom metadata value""""
  val defaultSuppliedFields: String = """"closure_type": "Open","description": "some kind of description""""

  def expectedSystemMetadata(filePath: String): List[MetadataProperty] = List(
    MetadataProperty("file_path", s"$filePath"),
    MetadataProperty("file_name", "file1.txt"),
    MetadataProperty("date_last_modified", "1751534387000"),
    MetadataProperty("file_size", s"$defaultFileSize"),
    MetadataProperty("client_side_checksum", "1b47903dfdf5f21abeb7b304efb8e801656bff31225f522406f45c21a68eddf2")
  )

  val expectedSuppliedMetadata: List[MetadataProperty] =
    List(MetadataProperty("description", "some kind of description"), MetadataProperty("closure status", "Open"))

  val expectedCustomMetadata: List[MetadataProperty] = List(MetadataProperty("customField", "custom metadata value"))

  def validBaseMetadataJsonString(matchId: String, consignmentId: UUID, filePath: String): String =
    s"""{
       | "file_size": "$defaultFileSize",
       | "transferId": "$consignmentId",
       | "file_path": "$filePath",
       | "matchId": "$matchId",
       | "date_last_modified": "1751534387000",
       | "file_name": "file1.txt",
       | "client_side_checksum": "1b47903dfdf5f21abeb7b304efb8e801656bff31225f522406f45c21a68eddf2"
       |}""".stripMargin

  def validBaseMetadataWithSuppliedAndCustom(matchId: String, consignmentId: UUID, filePath: String): String = {
    val baseJsonString = validBaseMetadataJsonString(matchId, consignmentId, filePath)
    addOptionalMetadataJsonString(baseJsonString, Some(defaultSuppliedFields), Some(customFieldJsonString))
  }

  def addOptionalMetadataJsonString(defaultJsonString: String, suppliedMetadataJson: Option[String], customMetadataJson: Option[String]): String = {
    val optionalJsonString = suppliedMetadataJson match {
      case _ if suppliedMetadataJson.nonEmpty && customMetadataJson.nonEmpty =>
        "," + suppliedMetadataJson.get + "," + customMetadataJson.get
      case _ if suppliedMetadataJson.nonEmpty =>
        "," + suppliedMetadataJson.get
      case _ if customMetadataJson.nonEmpty =>
        "," + customMetadataJson.get
      case _ => ""
    }

    val closingBraceIndex = defaultJsonString.lastIndexOf("}") - 1
    defaultJsonString.patch(closingBraceIndex, optionalJsonString, 0)
  }

  def droidMetadataJsonString(matchId: String, fileSize: Long, consignmentId: UUID, suppliedMetadataJson: Option[String], customMetadataJson: Option[String]): String = {
    val defaultJsonString = s"""{
                               | "SIZE": "$fileSize",
                               | "LAST_MODIFIED": "2025-07-03T09:19:47",
                               | "NAME": "file1.txt",
                               | "FILE_PATH": "Z:\\\\year_batch\\\\batch_number\\\\series\\\\Content\\\\Retail\\\\Shared Documents\\\\file1.txt",
                               | "SHA256_HASH": "1b47903dfdf5f21abeb7b304efb8e801656bff31225f522406f45c21a68eddf2",
                               | "matchId": "$matchId",
                               | "transferId": "$consignmentId"
    }""".stripMargin

    addOptionalMetadataJsonString(defaultJsonString, suppliedMetadataJson, customMetadataJson)
  }

  def hardDriveMetadataJsonString(matchId: String, fileSize: Long, consignmentId: UUID, suppliedMetadataJson: Option[String], customMetadataJson: Option[String]): String = {
    val defaultJsonString = s"""{
      "file_size": "$fileSize",
      "date_last_modified": "2025-07-03T09:19:47",
      "file_name": "file1.txt",
      "file_path": "Z:\\\\year_batch\\\\batch_number\\\\series\\\\content\\\\Retail\\\\Shared Documents\\\\file1.txt",
      "checksum": "1b47903dfdf5f21abeb7b304efb8e801656bff31225f522406f45c21a68eddf2",
      "matchId": "$matchId",
      "transferId": "$consignmentId"
    }""".stripMargin

    addOptionalMetadataJsonString(defaultJsonString, suppliedMetadataJson, customMetadataJson)
  }

  def networkDriveJsonString(matchId: String, fileSize: Long, consignmentId: UUID, suppliedMetadataJson: Option[String], customMetadataJson: Option[String]): String = {
    val defaultJsonString = s"""{
      "fileSize": "$fileSize",
      "transferId": "$consignmentId",
      "originalPath": "top-level/Retail/Shared Documents/file1.txt",
      "checksum": "1b47903dfdf5f21abeb7b304efb8e801656bff31225f522406f45c21a68eddf2",
      "lastModified": "1751534387000",
      "matchId": "$matchId"
    }""".stripMargin

    addOptionalMetadataJsonString(defaultJsonString, suppliedMetadataJson, customMetadataJson)
  }

  def sharePointMetadataJsonString(matchId: String, fileSize: Long, consignmentId: UUID, suppliedMetadataJson: Option[String], customMetadataJson: Option[String]): String = {
    val defaultJsonString = s"""{
      "Length": "$fileSize",
      "Modified": "2025-07-03T09:19:47Z",
      "FileLeafRef": "file1.txt",
      "FileRef": "/sites/Retail/Shared Documents/file1.txt",
      "sha256ClientSideChecksum": "1b47903dfdf5f21abeb7b304efb8e801656bff31225f522406f45c21a68eddf2",
      "matchId": "$matchId",
      "transferId": "$consignmentId"
    }""".stripMargin

    addOptionalMetadataJsonString(defaultJsonString, suppliedMetadataJson, customMetadataJson)
  }

  def convertStringToJson(jsonString: String): Json = {
    parser.parse(jsonString).fold(err => throw new RuntimeException(err.getMessage()), j => j)
  }
}
