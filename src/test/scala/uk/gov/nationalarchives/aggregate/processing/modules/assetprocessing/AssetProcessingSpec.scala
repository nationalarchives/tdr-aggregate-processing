package uk.gov.nationalarchives.aggregate.processing.modules.assetprocessing

import com.typesafe.scalalogging.Logger
import graphql.codegen.types.ClientSideMetadataInput
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar.{mock, times, verify, when}
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor6}
import org.slf4j.{Logger => UnderlyingLogger}
import software.amazon.awssdk.core.ResponseBytes
import software.amazon.awssdk.core.async.AsyncResponseTransformer
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model._
import software.amazon.awssdk.utils.CompletableFutureUtils.failedFuture
import uk.gov.nationalarchives.aggregate.processing.{ExternalServiceSpec, MetadataHelper}
import uk.gov.nationalarchives.aggregate.processing.config.ApplicationConfig.malwareScanKey
import uk.gov.nationalarchives.aggregate.processing.modules.Common.AssetSource
import uk.gov.nationalarchives.aggregate.processing.modules.assetprocessing.AssetProcessing.AssetProcessingResult
import uk.gov.nationalarchives.aggregate.processing.modules.assetprocessing.metadata.MetadataProperty
import uk.gov.nationalarchives.aws.utils.s3.S3Utils

import java.io.ByteArrayInputStream
import java.util.UUID
import java.util.concurrent.CompletableFuture

class AssetProcessingSpec extends ExternalServiceSpec with TableDrivenPropertyChecks with MetadataHelper {
  private val userId = UUID.randomUUID()
  private val consignmentId = UUID.randomUUID()
  private val matchId = UUID.randomUUID().toString

  val assetSources: TableFor6[String, (String, Long, UUID, Option[String], Option[String]) => String, String, String, String, String] = Table(
    ("Source", "Default Metadata Json String", "Expected Parsed File Path", "Missing Required Field", "Supplied Metadata Json", "Custom Metadata Json"),
    (
      AssetSource.HardDrive.toString.toLowerCase,
      hardDriveMetadataJsonString,
      "content/Retail/Shared Documents/file1.txt",
      "file_size",
      defaultSuppliedField,
      customFieldJsonString
    ),
    (AssetSource.NetworkDrive.toString.toLowerCase, networkDriveJsonString, "top-level/Retail/Shared Documents/file1.txt", "fileSize", defaultSuppliedField, customFieldJsonString),
    (
      AssetSource.SharePoint.toString.toLowerCase,
      sharePointMetadataJsonString,
      "sites/Retail/Shared Documents/file1.txt",
      "Length",
      """"closure_x0020_status": "open"""",
      customFieldJsonString
    )
  )

  forAll(assetSources) { (assetSource, defaultMetadataJsonString, expectedParsedFilePath, missingRequiredField, suppliedFieldJson, customFieldJson) =>
    s"'processAsset' with asset source of $assetSource" should
      "return asset processing result and not log errors when metadata json is valid" in {
        val mockLogger = mock[UnderlyingLogger]
        val s3UtilsMock = mock[S3Utils]

        val expectedInput = ClientSideMetadataInput(
          s"$expectedParsedFilePath",
          "1b47903dfdf5f21abeb7b304efb8e801656bff31225f522406f45c21a68eddf2",
          1751534387000L,
          defaultFileSize,
          matchId
        )

        val expectedResult = AssetProcessingResult(
          Some(matchId),
          processingErrors = false,
          Some(expectedInput),
          systemMetadata = List(
            MetadataProperty("file_path", s"$expectedParsedFilePath"),
            MetadataProperty("file_name", "file1.txt"),
            MetadataProperty("date_last_modified", "1751534387000"),
            MetadataProperty("file_size", s"${defaultFileSize.toString}"),
            MetadataProperty("client_side_checksum", "1b47903dfdf5f21abeb7b304efb8e801656bff31225f522406f45c21a68eddf2")
          )
        )

        when(mockLogger.isInfoEnabled()).thenReturn(true)
        when(mockLogger.isErrorEnabled).thenReturn(true)
        when(s3UtilsMock.getObjectAsStream(any[String], any[String]))
          .thenReturn(new ByteArrayInputStream(defaultMetadataJsonString(matchId, defaultFileSize, consignmentId, None, None).getBytes("UTF-8")))
        when(s3UtilsMock.getObjectTags(any[String], any[String]))
          .thenReturn(Map(malwareScanKey -> "NO_THREATS_FOUND"))

        val assetProcessing = new AssetProcessing(s3UtilsMock)(Logger(mockLogger))
        val result = assetProcessing.processAsset("s3Bucket", s"$userId/$assetSource/$consignmentId/metadata/$matchId.metadata")

        result shouldEqual expectedResult

        verify(s3UtilsMock, times(1)).addObjectTags("s3Bucket", s"$userId/$assetSource/$consignmentId/metadata/$matchId.metadata", Map("ASSET_PROCESSING" -> "Completed"))

        verify(mockLogger, times(0)).isErrorEnabled
        verify(mockLogger).info("Asset metadata successfully processed for: {}", s"$userId/$assetSource/$consignmentId/metadata/$matchId.metadata")
      }

    s"'processAsset' with asset source of $assetSource" should
      "log an error and return asset processing result when the object key is invalid" in {
        val mockLogger = mock[UnderlyingLogger]
        val s3UtilsMock = mock[S3Utils]
        val expectedResult = AssetProcessingResult(None, processingErrors = true, None)

        when(mockLogger.isInfoEnabled()).thenReturn(true)
        when(mockLogger.isErrorEnabled()).thenReturn(true)
        when(s3UtilsMock.getObjectTags(any[String], any[String]))
          .thenReturn(Map(malwareScanKey -> "NO_THREATS_FOUND"))

        val assetProcessing = new AssetProcessing(s3UtilsMock)(Logger(mockLogger))

        val result = assetProcessing.processAsset("s3Bucket", "incorrect/object/key/format.txt")

        result shouldEqual expectedResult

        verify(s3UtilsMock, times(1)).addObjectTags("s3Bucket", "incorrect/object/key/format.txt", Map("ASSET_PROCESSING" -> "CompletedWithIssues"))

        verify(mockLogger).error(
          s"AssetProcessingError: consignmentId: None, matchId: None, source: None, errorCode: ASSET_PROCESSING.OBJECT_KEY.INVALID, errorMessage: Invalid object key: incorrect/object/key/format.txt: Invalid UUID string: incorrect"
        )
      }

    s"'processAsset' with asset source of $assetSource" should
      "log an error and return asset processing result when a required json field is missing" in {
        val mockLogger = mock[UnderlyingLogger]
        val s3UtilsMock = mock[S3Utils]
        val missingFieldJson = defaultMetadataJsonString(matchId, defaultFileSize, consignmentId, None, None).replace(s""""$missingRequiredField": "12",""", "")
        val expectedResult = AssetProcessingResult(Some(matchId), processingErrors = true, None)

        when(mockLogger.isErrorEnabled()).thenReturn(true)
        when(mockLogger.isInfoEnabled()).thenReturn(true)
        when(s3UtilsMock.getObjectTags(any[String], any[String]))
          .thenReturn(Map(malwareScanKey -> "NO_THREATS_FOUND"))
        when(s3UtilsMock.getObjectAsStream(any[String], any[String]))
          .thenReturn(new ByteArrayInputStream(missingFieldJson.getBytes("UTF-8")))

        val assetProcessing = new AssetProcessing(s3UtilsMock)(Logger(mockLogger))
        val result = assetProcessing.processAsset("s3Bucket", s"$userId/$assetSource/$consignmentId/metadata/$matchId.metadata")

        result shouldEqual expectedResult

        verify(s3UtilsMock, times(1)).addObjectTags("s3Bucket", s"$userId/$assetSource/$consignmentId/metadata/$matchId.metadata", Map("ASSET_PROCESSING" -> "CompletedWithIssues"))

        verify(mockLogger).error(
          s"AssetProcessingError: consignmentId: Some($consignmentId), matchId: Some($matchId), source: Some($assetSource), errorCode: ASSET_PROCESSING.JSON.INVALID, errorMessage: DecodingFailure at .file_size: Missing required field"
        )
      }

    s"'processAsset' with asset source of $assetSource" should
      "log an error and return asset processing result when reading S3 object stream fails" in {
        val mockLogger = mock[UnderlyingLogger]
        val s3AsyncClient = mock[S3AsyncClient]
        val s3UtilsMock = S3Utils(s3AsyncClient)

        val tag = Tag.builder().key(malwareScanKey).value("NO_THREATS_FOUND").build()
        val objectTaggingResponse = GetObjectTaggingResponse
          .builder()
          .tagSet(tag)
          .build()

        when(mockLogger.isErrorEnabled()).thenReturn(true)
        when(mockLogger.isInfoEnabled()).thenReturn(true)
        when(s3AsyncClient.getObjectTagging(any[GetObjectTaggingRequest]))
          .thenReturn(CompletableFuture.completedFuture(objectTaggingResponse))
        when(
          s3AsyncClient.getObject(
            any(classOf[GetObjectRequest]),
            any(classOf[AsyncResponseTransformer[GetObjectResponse, ResponseBytes[GetObjectResponse]]])
          )
        ).thenReturn(failedFuture(new RuntimeException("read failed")))

        val expectedResult = AssetProcessingResult(Some(matchId), processingErrors = true, None)

        val assetProcessing = new AssetProcessing(s3UtilsMock)(Logger(mockLogger))
        val result = assetProcessing.processAsset("s3Bucket", s"$userId/$assetSource/$consignmentId/metadata/$matchId.metadata")

        result shouldEqual expectedResult

        verify(s3AsyncClient, times(2)).getObjectTagging(
          GetObjectTaggingRequest
            .builder()
            .bucket("s3Bucket")
            .key(s"$userId/$assetSource/$consignmentId/metadata/$matchId.metadata")
            .build()
        )

        verify(mockLogger).error(
          s"AssetProcessingError: consignmentId: Some($consignmentId), matchId: Some($matchId), source: Some($assetSource), errorCode: ASSET_PROCESSING.S3.READ_ERROR, errorMessage: java.lang.RuntimeException: read failed"
        )
      }

    s"'processAsset' with asset source of $assetSource" should
      "log an error and return asset processing result when S3 object is non-UTF-8 encoded" in {
        val mockLogger = mock[UnderlyingLogger]
        val s3UtilsMock = mock[S3Utils]
        val invalidUtf8 = Array[Byte](0, -1, -2, -3)
        val expectedResult = AssetProcessingResult(Some(matchId), processingErrors = true, None)

        when(mockLogger.isErrorEnabled()).thenReturn(true)
        when(mockLogger.isInfoEnabled()).thenReturn(true)
        when(s3UtilsMock.getObjectTags(any[String], any[String]))
          .thenReturn(Map(malwareScanKey -> "NO_THREATS_FOUND"))
        when(s3UtilsMock.getObjectAsStream(any[String], any[String]))
          .thenReturn(new ByteArrayInputStream(invalidUtf8))

        val assetProcessing = new AssetProcessing(s3UtilsMock)(Logger(mockLogger))
        val result = assetProcessing.processAsset("s3Bucket", s"$userId/$assetSource/$consignmentId/metadata/$matchId.metadata")

        result shouldEqual expectedResult

        verify(s3UtilsMock, times(1)).addObjectTags("s3Bucket", s"$userId/$assetSource/$consignmentId/metadata/$matchId.metadata", Map("ASSET_PROCESSING" -> "CompletedWithIssues"))

        verify(mockLogger).error(
          s"AssetProcessingError: consignmentId: Some($consignmentId), matchId: Some($matchId), source: Some($assetSource), errorCode: ASSET_PROCESSING.ENCODING.INVALID, errorMessage: Invalid UTF-8 Sequence, expecting: 4bytes, but got: 3bytes - reached end of stream. @ byte position: -1"
        )
      }

    s"'processAsset' with asset source of $assetSource" should
      "log an error and return asset processing result when S3 object is not valid json" in {
        val mockLogger = mock[UnderlyingLogger]
        val s3UtilsMock = mock[S3Utils]
        val nonJsonString = "some random string".getBytes("UTF-8")
        val expectedResult = AssetProcessingResult(Some(matchId), processingErrors = true, None)

        when(mockLogger.isErrorEnabled()).thenReturn(true)
        when(mockLogger.isInfoEnabled()).thenReturn(true)
        when(s3UtilsMock.getObjectTags(any[String], any[String]))
          .thenReturn(Map(malwareScanKey -> "NO_THREATS_FOUND"))
        when(s3UtilsMock.getObjectAsStream(any[String], any[String]))
          .thenReturn(new ByteArrayInputStream(nonJsonString))

        val assetProcessing = new AssetProcessing(s3UtilsMock)(Logger(mockLogger))
        val result = assetProcessing.processAsset("s3Bucket", s"$userId/$assetSource/$consignmentId/metadata/$matchId.metadata")

        result shouldEqual expectedResult

        verify(s3UtilsMock, times(1)).addObjectTags("s3Bucket", s"$userId/$assetSource/$consignmentId/metadata/$matchId.metadata", Map("ASSET_PROCESSING" -> "CompletedWithIssues"))

        verify(mockLogger).error(
          s"AssetProcessingError: consignmentId: Some($consignmentId), matchId: Some($matchId), source: Some($assetSource), errorCode: ASSET_PROCESSING.JSON.INVALID, errorMessage: expected json value got 'some r...' (line 1, column 1)"
        )
      }

    s"'processAsset' with asset source of $assetSource" should
      "log an error and return asset processing result when json is an unknown structure" in {
        val mockLogger = mock[UnderlyingLogger]
        val s3UtilsMock = mock[S3Utils]
        val unknownJsonString = s"""{"aField": "aValue", "bField": "bValue}"""
        val expectedResult = AssetProcessingResult(Some(matchId), processingErrors = true, None)

        when(mockLogger.isErrorEnabled()).thenReturn(true)
        when(mockLogger.isInfoEnabled()).thenReturn(true)
        when(s3UtilsMock.getObjectTags(any[String], any[String]))
          .thenReturn(Map(malwareScanKey -> "NO_THREATS_FOUND"))
        when(s3UtilsMock.getObjectAsStream(any[String], any[String]))
          .thenReturn(new ByteArrayInputStream(unknownJsonString.getBytes("UTF-8")))

        val assetProcessing = new AssetProcessing(s3UtilsMock)(Logger(mockLogger))
        val result = assetProcessing.processAsset("s3Bucket", s"$userId/$assetSource/$consignmentId/metadata/$matchId.metadata")

        result shouldEqual expectedResult

        verify(s3UtilsMock, times(1)).addObjectTags("s3Bucket", s"$userId/$assetSource/$consignmentId/metadata/$matchId.metadata", Map("ASSET_PROCESSING" -> "CompletedWithIssues"))

        verify(mockLogger).error(
          s"AssetProcessingError: consignmentId: Some($consignmentId), matchId: Some($matchId), source: Some($assetSource), errorCode: ASSET_PROCESSING.JSON.INVALID, errorMessage: exhausted input"
        )
      }

    s"'processAsset' with asset source of $assetSource" should
      "log an error and return asset processing result when the event matchId does not match the metadata matchId" in {
        val mockLogger = mock[UnderlyingLogger]
        val s3UtilsMock = mock[S3Utils]
        val differentMatchId = UUID.randomUUID()
        val expectedResult = AssetProcessingResult(None, processingErrors = true, None)

        when(mockLogger.isErrorEnabled()).thenReturn(true)
        when(mockLogger.isInfoEnabled()).thenReturn(true)
        when(s3UtilsMock.getObjectTags(any[String], any[String]))
          .thenReturn(Map(malwareScanKey -> "NO_THREATS_FOUND"))
        when(s3UtilsMock.getObjectAsStream(any[String], any[String]))
          .thenReturn(new ByteArrayInputStream(defaultMetadataJsonString(matchId, defaultFileSize, consignmentId, None, None).getBytes("UTF-8")))

        val assetProcessing = new AssetProcessing(s3UtilsMock)(Logger(mockLogger))
        val result = assetProcessing.processAsset("s3Bucket", s"$userId/$assetSource/$consignmentId/metadata/$differentMatchId.metadata")

        result shouldEqual expectedResult

        verify(s3UtilsMock, times(1)).addObjectTags(
          "s3Bucket",
          s"$userId/$assetSource/$consignmentId/metadata/$differentMatchId.metadata",
          Map("ASSET_PROCESSING" -> "CompletedWithIssues")
        )

        verify(mockLogger).error(
          s"AssetProcessingError: consignmentId: Some($consignmentId), matchId: None, source: Some($assetSource), errorCode: ASSET_PROCESSING.MATCH_ID.MISMATCH, errorMessage: Mismatched match ids: $differentMatchId and $matchId"
        )
      }

    s"'processAsset' with asset source of $assetSource" should
      "log an error and return asset processing result when malware threat is found" in {
        val mockLogger = mock[UnderlyingLogger]
        val s3UtilsMock = mock[S3Utils]

        import uk.gov.nationalarchives.aggregate.processing.config.ApplicationConfig.malwareScanThreatFound

        when(mockLogger.isErrorEnabled()).thenReturn(true)
        when(mockLogger.isInfoEnabled()).thenReturn(true)
        when(s3UtilsMock.getObjectTags(any[String], any[String]))
          .thenReturn(Map(malwareScanKey -> malwareScanThreatFound))

        val assetProcessing = new AssetProcessing(s3UtilsMock)(Logger(mockLogger))
        val result = assetProcessing.processAsset("s3Bucket", s"$userId/$assetSource/$consignmentId/metadata/$matchId.metadata")

        val expectedResult = AssetProcessingResult(Some(matchId), processingErrors = true, None)
        result shouldEqual expectedResult

        verify(s3UtilsMock, times(1)).addObjectTags(
          "s3Bucket",
          s"$userId/$assetSource/$consignmentId/metadata/$matchId.metadata",
          Map("ASSET_PROCESSING" -> "CompletedWithIssues")
        )

        verify(mockLogger).error(
          s"AssetProcessingError: consignmentId: Some($consignmentId), matchId: Some($matchId), source: Some($assetSource), errorCode: ASSET_PROCESSING.MALWARE_SCAN.THREAT_FOUND, errorMessage: malware scan threat found"
        )
      }

    s"'processAsset' with asset source of $assetSource" should
      "return asset processing result and an log error when metadata fails initial checks" in {
        val mockLogger = mock[UnderlyingLogger]
        val s3UtilsMock = mock[S3Utils]

        val expectedInput = ClientSideMetadataInput(
          expectedParsedFilePath,
          "1b47903dfdf5f21abeb7b304efb8e801656bff31225f522406f45c21a68eddf2",
          1751534387000L,
          0L,
          matchId
        )

        val expectedResult = AssetProcessingResult(
          Some(matchId),
          processingErrors = true,
          Some(expectedInput)
        )

        when(mockLogger.isInfoEnabled()).thenReturn(true)
        when(mockLogger.isErrorEnabled).thenReturn(true)
        when(s3UtilsMock.getObjectTags(any[String], any[String]))
          .thenReturn(Map(malwareScanKey -> "NO_THREATS_FOUND"))
        when(s3UtilsMock.getObjectAsStream(any[String], any[String]))
          .thenReturn(new ByteArrayInputStream(defaultMetadataJsonString(matchId, 0, consignmentId, None, None).getBytes("UTF-8")))

        val assetProcessing = new AssetProcessing(s3UtilsMock)(Logger(mockLogger))
        val result = assetProcessing.processAsset("s3Bucket", s"$userId/$assetSource/$consignmentId/metadata/$matchId.metadata")

        result shouldEqual expectedResult

        verify(s3UtilsMock, times(1)).addObjectTags(
          "s3Bucket",
          s"$userId/$assetSource/$consignmentId/metadata/$matchId.metadata",
          Map("ASSET_PROCESSING" -> "CompletedWithIssues")
        )

        verify(mockLogger).error(
          s"AssetProcessingError: consignmentId: Some($consignmentId), matchId: Some($matchId), source: Some($assetSource), errorCode: INITIAL_CHECKS.OBJECT_SIZE.TOO_SMALL, errorMessage: File size: 0 bytes"
        )
      }

    s"'processAsset' with asset source of $assetSource" should
      "return asset processing result and not log errors when supplied metadata json is valid" in {
        val mockLogger = mock[UnderlyingLogger]
        val s3UtilsMock = mock[S3Utils]
        val jsonMetadataString = defaultMetadataJsonString(matchId, defaultFileSize, consignmentId, Some(suppliedFieldJson), None)

        val expectedInput = ClientSideMetadataInput(
          expectedParsedFilePath,
          "1b47903dfdf5f21abeb7b304efb8e801656bff31225f522406f45c21a68eddf2",
          1751534387000L,
          12L,
          matchId
        )

        val expectedResult =
          AssetProcessingResult(
            Some(matchId),
            processingErrors = false,
            Some(expectedInput),
            systemMetadata = List(
              MetadataProperty("file_path", s"$expectedParsedFilePath"),
              MetadataProperty("file_name", "file1.txt"),
              MetadataProperty("date_last_modified", "1751534387000"),
              MetadataProperty("file_size", s"$defaultFileSize"),
              MetadataProperty("client_side_checksum", "1b47903dfdf5f21abeb7b304efb8e801656bff31225f522406f45c21a68eddf2")
            ),
            suppliedMetadata = expectedSuppliedMetadata
          )

        when(mockLogger.isInfoEnabled()).thenReturn(true)
        when(mockLogger.isErrorEnabled).thenReturn(true)
        when(s3UtilsMock.getObjectTags(any[String], any[String]))
          .thenReturn(Map(malwareScanKey -> "NO_THREATS_FOUND"))
        when(s3UtilsMock.getObjectAsStream(any[String], any[String]))
          .thenReturn(new ByteArrayInputStream(jsonMetadataString.getBytes("UTF-8")))

        val assetProcessing = new AssetProcessing(s3UtilsMock)(Logger(mockLogger))
        val result = assetProcessing.processAsset("s3Bucket", s"$userId/$assetSource/$consignmentId/metadata/$matchId.metadata")

        result shouldEqual expectedResult

        verify(mockLogger, times(0)).isErrorEnabled
        verify(mockLogger).info("Asset metadata successfully processed for: {}", s"$userId/$assetSource/$consignmentId/metadata/$matchId.metadata")
      }

    s"'processAsset' with asset source of $assetSource" should
      "return asset processing result and not log errors when json contains custom metadata" in {
        val mockLogger = mock[UnderlyingLogger]
        val s3UtilsMock = mock[S3Utils]
        val jsonMetadataString = defaultMetadataJsonString(matchId, defaultFileSize, consignmentId, Some(suppliedFieldJson), Some(customFieldJson))

        val expectedInput = ClientSideMetadataInput(
          expectedParsedFilePath,
          "1b47903dfdf5f21abeb7b304efb8e801656bff31225f522406f45c21a68eddf2",
          1751534387000L,
          12L,
          matchId
        )

        val expectedResult =
          AssetProcessingResult(
            Some(matchId),
            processingErrors = false,
            Some(expectedInput),
            systemMetadata = List(
              MetadataProperty("file_path", s"$expectedParsedFilePath"),
              MetadataProperty("file_name", "file1.txt"),
              MetadataProperty("date_last_modified", "1751534387000"),
              MetadataProperty("file_size", s"$defaultFileSize"),
              MetadataProperty("client_side_checksum", "1b47903dfdf5f21abeb7b304efb8e801656bff31225f522406f45c21a68eddf2")
            ),
            suppliedMetadata = expectedSuppliedMetadata,
            customMetadata = expectedCustomMetadata
          )

        when(mockLogger.isInfoEnabled()).thenReturn(true)
        when(mockLogger.isErrorEnabled).thenReturn(true)
        when(s3UtilsMock.getObjectTags(any[String], any[String]))
          .thenReturn(Map(malwareScanKey -> "NO_THREATS_FOUND"))
        when(s3UtilsMock.getObjectAsStream(any[String], any[String]))
          .thenReturn(new ByteArrayInputStream(jsonMetadataString.getBytes("UTF-8")))

        val assetProcessing = new AssetProcessing(s3UtilsMock)(Logger(mockLogger))
        val result = assetProcessing.processAsset("s3Bucket", s"$userId/$assetSource/$consignmentId/metadata/$matchId.metadata")

        result shouldEqual expectedResult

        verify(mockLogger, times(0)).isErrorEnabled
        verify(mockLogger).info("Asset metadata successfully processed for: {}", s"$userId/$assetSource/$consignmentId/metadata/$matchId.metadata")
      }
  }
}
