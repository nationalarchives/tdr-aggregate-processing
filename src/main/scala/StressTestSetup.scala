import cats.effect.unsafe.implicits.global
import graphql.codegen.types.ConsignmentStatusInput
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.model.{PutObjectRequest, Tag, Tagging}
import uk.gov.nationalarchives.aggregate.processing.persistence.GraphQlApi
import uk.gov.nationalarchives.aggregate.processing.persistence.GraphQlApi.{backend, keycloakDeployment}
import uk.gov.nationalarchives.aws.utils.s3.S3Clients.s3

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.UUID

object StressTestSetup extends App {

  /*
   * Instructions
   * 1. Temporarily disable the AWS GuardDuty malware scan on the S3 bucket: AWS Console -> GuardDuty -> Malware Protection -> S3
   * 2. In the TDR application start a transfer in the desired environment (intg / staging), noting down the consignment id
   * 3. Replace the 'placeholder' values with the correct values for the transfer
   * 4. Set the 'numberOfAssets' to the desired number of files to process
   * 5. Add AWS credentials giving permission to put objects into the relevant S3 bucket
   * 6. Run the app to upload the generated records / metadata to the S3 bucket
   * 7. Send SQS message to trigger the lambda to process the consignment
   * 8. Enable AWS GuardDuty malware scan on the S3 bucket: AWS Console -> GuardDuty -> Malware Protection -> S3
   * */

  val s3Client = s3("https://s3.eu-west-2.amazonaws.com/")
  val s3OutputBucket = "tdr-upload-files-cloudfront-dirty-staging"
  val numberOfAssets = 10000
  val userId = "7ce3cd87-66b9-4d9e-aa38-183f4d84fa0c"
  val consignmentId = "2e8103ca-2ac5-4159-9089-8b055d722b94"
  val malwareScanTag = Tagging
    .builder()
    .tagSet(
      Tag
        .builder()
        .key("GuardDutyMalwareScanStatus")
        .value("NO_THREATS_FOUND")
        .build()
    )
    .build()

  GraphQlApi().addConsignmentStatus(ConsignmentStatusInput(
    consignmentId = UUID.fromString(consignmentId),
    statusType = "Upload",
    statusValue = Some("InProgress"),
    userIdOverride = Some(UUID.fromString(userId))
  )).unsafeRunSync()

  (1 to numberOfAssets).foreach { i =>
    println("Uploading asset " + i)
    val matchId = UUID.randomUUID()
    val fileName = s"file$i.txt"
    val filePath = s"/sites/Retail/Shared Documents/$fileName"
    val inputStream = mockFile(filePath)
    val checksum = generateCheckSum(inputStream)
    inputStream.reset()

    val fileContent = inputStream.readAllBytes().map(_.toChar).mkString
    val fileSize = fileContent.getBytes().length
    val fileBody = RequestBody.fromString(fileContent)
    val fileRequest = PutObjectRequest.builder.bucket(s3OutputBucket).key(s"$userId/sharepoint/$consignmentId/records/$matchId").build

    s3Client.putObject(fileRequest, fileBody)

    val metadataJsonString =
      s"""{
         |  "Length": "$fileSize",
         |  "Modified": "2025-07-03T09:19:47Z",
         |  "FileLeafRef": "$fileName",
         |  "FileRef": "$filePath",
         |  "sha256ClientSideChecksum": "$checksum",
         |  "matchId": "$matchId",
         |  "transferId": "$consignmentId",
         |  "description": "This is the description for $fileName",
         |  "custom1": "Custom metadata 1",
         |  "custom2": "Custom metadata 2",
         |  "custom3": "Custom metadata 3",
         |  "custom4": "Custom metadata 4",
         |  "custom5": "Custom metadata 5",
         |  "custom6": "Custom metadata 6",
         |  "custom7": "Custom metadata 7",
         |  "custom8": "Custom metadata 8",
         |  "custom9": "Custom metadata 9",
         |  "custom10": "Custom metadata 10"
         |}
         |""".stripMargin

    val metadataBody = RequestBody.fromString(metadataJsonString)
    val metadataRequest = PutObjectRequest.builder.bucket(s3OutputBucket).key(s"$userId/sharepoint/$consignmentId/metadata/$matchId.metadata").build
    s3Client.putObject(metadataRequest, metadataBody)
  }

  private def mockFile(filePath: String) = {
    val text = s"File: $filePath"
    new ByteArrayInputStream(text.getBytes(StandardCharsets.UTF_8))
  }

  private def generateCheckSum(inputStream: ByteArrayInputStream): String = {
    val messageDigester: MessageDigest = MessageDigest.getInstance("SHA-256")
    messageDigester.digest(inputStream.readAllBytes()).map(byte => f"$byte%02x").mkString
  }
}
