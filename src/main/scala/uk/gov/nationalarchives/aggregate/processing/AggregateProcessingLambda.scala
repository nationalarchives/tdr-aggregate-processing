package uk.gov.nationalarchives.aggregate.processing

import cats.effect.IO
import cats.effect.IO._
import cats.effect.unsafe.implicits.global
import com.amazonaws.services.lambda.runtime.events.SQSEvent
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage
import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import com.github.tototoshi.csv.CSVWriter
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.Logger
import graphql.codegen.types.AddFileAndMetadataInput
import io.circe._
import io.circe.generic.semiauto.deriveDecoder
import io.circe.parser._
import uk.gov.nationalarchives.aggregate.processing.AggregateProcessingLambda._
import uk.gov.nationalarchives.aggregate.processing.config.ApplicationConfig.draftMetadataBucket
import uk.gov.nationalarchives.aggregate.processing.modules.Common.ObjectCategory.ObjectCategory
import uk.gov.nationalarchives.aggregate.processing.modules.Common.ProcessErrorType.{ClientDataLoadError, S3Error}
import uk.gov.nationalarchives.aggregate.processing.modules.Common.ProcessErrorValue.{Failure, ReadError}
import uk.gov.nationalarchives.aggregate.processing.modules.Common.ProcessType.AggregateProcessing
import uk.gov.nationalarchives.aggregate.processing.modules.ErrorHandling.{BaseError, handleError}
import uk.gov.nationalarchives.aggregate.processing.modules.TransferOrchestration.{AggregateProcessingEvent, OrchestrationResult}
import uk.gov.nationalarchives.aggregate.processing.modules.{AssetProcessing, Common, TransferOrchestration}
import uk.gov.nationalarchives.aggregate.processing.persistence.GraphQlApi
import uk.gov.nationalarchives.aggregate.processing.persistence.GraphQlApi.{backend, keycloakDeployment}
import uk.gov.nationalarchives.aws.utils.s3.{S3Clients, S3Utils}
import uk.gov.nationalarchives.tdr.schemautils.ConfigUtils

import java.io.File
import java.nio.file.{Files, Path}
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.UUID
import scala.collection.immutable.Map
import scala.jdk.CollectionConverters.CollectionHasAsScala

class AggregateProcessingLambda extends RequestHandler[SQSEvent, Unit] {
  implicit val sharepointInputDecoder: Decoder[AggregateEvent] = deriveDecoder[AggregateEvent]
  private val assetProcessor = AssetProcessing()
  private val persistenceApi = GraphQlApi()
  private val orchestrator = TransferOrchestration()
  private lazy val errorProcessingResult = AssetProcessingResult(errors = true, suppliedMetadata = false)

  override def handleRequest(event: SQSEvent, context: Context): Unit = {
    val sqsMessages: Seq[SQSMessage] = event.getRecords.asScala.toList
    val events = sqsMessages.map(message => parseSqsMessage(message))

    val resultsIO = events.map(event =>
      processEvent(event).handleErrorWith(err => {
        val details = Common.objectKeyParser(event.metadataSourceObjectPrefix)
        val error = AggregateProcessingError(details.consignmentId, s"$AggregateProcessing", err.getMessage)
        handleError(error, logger)
        IO.unit
      })
    )

    resultsIO.parSequence.unsafeRunSync()
  }

  def processEvent(event: AggregateEvent): IO[OrchestrationResult] = {
    val sourceBucket = event.metadataSourceBucket
    val objectsPrefix = event.metadataSourceObjectPrefix
    val objectKeyPrefixDetails = Common.objectKeyParser(objectsPrefix)
    val consignmentId = objectKeyPrefixDetails.consignmentId
    val userId = objectKeyPrefixDetails.userId
    val dataLoadErrors = event.dataLoadErrors
    val assetSource = objectKeyPrefixDetails.assetSource
    logger.info(s"Starting processing consignment: $consignmentId")
    for {
      s3Objects <- IO(s3Utils.listAllObjectsWithPrefix(sourceBucket, objectsPrefix))
      objectKeys = s3Objects.map(_.key())
      assetProcessingResult <- dataLoadErrors match {
        case _ if dataLoadErrors =>
          handleError(dataLoadError(consignmentId), logger)
          IO(errorProcessingResult)
        case _ if objectKeys.isEmpty =>
          handleError(noObjectsError(consignmentId, objectKeyPrefixDetails.category), logger)
          IO(errorProcessingResult)
        case _ => processAssets(userId, consignmentId, sourceBucket, objectKeys)
      }
      orchestrationEvent = AggregateProcessingEvent(
        assetSource,
        userId,
        consignmentId,
        processingErrors = assetProcessingResult.errors,
        suppliedMetadata = assetProcessingResult.suppliedMetadata
      )
      orchestrationResult <- orchestrator.orchestrate(orchestrationEvent)
    } yield orchestrationResult
  }

  private def dataLoadError(consignmentId: UUID) =
    AggregateProcessingError(consignmentId, s"$AggregateProcessing.$ClientDataLoadError.$Failure", s"Client data load errors for consignment: $consignmentId")

  private def noObjectsError(consignmentId: UUID, objectCategory: ObjectCategory) =
    AggregateProcessingError(consignmentId, s"$AggregateProcessing.$S3Error.$ReadError", s"No $objectCategory objects found for consignment: $consignmentId")

  private def processAssets(userId: UUID, consignmentId: UUID, sourceBucket: String, objectKeys: List[String]): IO[AssetProcessingResult] = {
    for {
      assetProcessingResults <- IO(objectKeys.map(assetProcessor.processAsset(sourceBucket, _)))
      assetProcessingErrors = assetProcessingResults.exists(_.processingErrors)
      suppliedMetadata = assetProcessingResults.exists(_.suppliedMetadata.nonEmpty)
      _ = if (suppliedMetadata) {
        val metadataCSV = createMetadataCSV(assetProcessingResults)
        uploadToS3(metadataCSV.toPath, draftMetadataBucket, s"/$consignmentId/draft-metadata.csv")
      }
      _ <-
        if (assetProcessingErrors) {
          IO(Nil)
        } else {
          val clientSideMetadataInput = assetProcessingResults.flatMap(_.clientSideMetadataInput)
          val input = AddFileAndMetadataInput(consignmentId, clientSideMetadataInput, None, Some(userId))
          persistenceApi.addClientSideMetadata(input)
        }
    } yield AssetProcessingResult(assetProcessingErrors, suppliedMetadata)
  }

  private def createMetadataCSV(assetProcessingResults: List[AssetProcessing.AssetProcessingResult]): File = {
    val metadataConfiguration = ConfigUtils.loadConfiguration
    val downloadDisplayProperties = metadataConfiguration
      .downloadFileDisplayProperties("MetadataDownloadTemplate")
      .sortBy(_.columnIndex)

    val keyToTdrFileHeader = metadataConfiguration.propertyToOutputMapper("tdrFileHeader")
    val keyToSharepointHeader = metadataConfiguration.propertyToOutputMapper("sharePointTag")
    val tdrHeaderToKey = metadataConfiguration.inputToPropertyMapper("tdrFileHeader")
    val sharepointHeaderToKey = metadataConfiguration.inputToPropertyMapper("sharePointTag")

    // --- Determine headers ---
    val dynamicHeaders: List[String] = downloadDisplayProperties.map(dp => keyToTdrFileHeader(dp.key))
    val colHeaders: List[String] = dynamicHeaders

    // --- Build rows ---
    val rows: List[List[String]] = assetProcessingResults.map { result =>
      val propertiesDefaultValues = metadataConfiguration.getPropertiesWithDefaultValue.map { prop =>
        val propertyType = metadataConfiguration.getPropertyType(prop._1)
        val convertedValue = convertValue(propertyType, prop._2)
        keyToTdrFileHeader(prop._1) -> convertedValue
      }
      val suppliedMetadataMap: Map[String, String] = result.suppliedMetadata.map { sm =>
        val propertyType = metadataConfiguration.getPropertyType(tdrHeaderToKey(sm.propertyName))
        val convertedValue = convertValue(propertyType, sm.propertyValue)
        sm.propertyName -> convertedValue
      }.toMap
      val systemMetadataMap: Map[String, String] = result.systemMetadata.map { sm =>
        val propertyType = metadataConfiguration.getPropertyType(sharepointHeaderToKey(sm.propertyName))
        val convertedValue = convertValue(propertyType, sm.propertyValue)
        keyToTdrFileHeader(sharepointHeaderToKey(sm.propertyName)) -> convertedValue
      }.toMap

      val defaultValues: List[String] = dynamicHeaders.map(header => propertiesDefaultValues.getOrElse(header, ""))
      val suppliedValues: List[String] = dynamicHeaders.map(header => suppliedMetadataMap.getOrElse(header, ""))
      val systemValues: List[String] = dynamicHeaders.map(header => systemMetadataMap.getOrElse(header, ""))

      List(suppliedValues, defaultValues, systemValues).transpose
        .map(_.find(_.nonEmpty).getOrElse(""))
    }

    // --- Write CSV ---
    val csvFile = Files.createTempFile("draft-metadata", ".csv").toFile
    val writer = CSVWriter.open(csvFile)

    try {
      writer.writeRow(colHeaders)
      writer.writeAll(rows)
    } finally {
      writer.close()
    }
    csvFile
  }

  private def convertValue(propertyType: String, fileMetadataValue: String): String = {
    propertyType match {
      case "date"    => convertToLocalDateOrString(fileMetadataValue)
      case "boolean" => if (fileMetadataValue == "true") "Yes" else "No"
//      case "integer"                   => Integer.valueOf(fileMetadataValue)
      case _ => fileMetadataValue
    }
  }

  private def convertToLocalDateOrString(timestampString: String): String = {
    val parsedDate = ZonedDateTime.parse(timestampString)
    parsedDate.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
  }

  private def uploadToS3(filePath: Path, bucket: String, key: String): Unit = {
    s3Utils.upload(bucket, key, filePath)
  }

  private def parseSqsMessage(sqsMessage: SQSMessage): AggregateEvent = {
    parse(sqsMessage.getBody) match {
      case Left(parsingError) =>
        throw new IllegalArgumentException(s"Invalid JSON object: ${parsingError.message}")
      case Right(json) =>
        decode[AggregateEvent](json.toString()) match {
          case Left(decodingError) => throw new IllegalArgumentException(s"Invalid event: ${decodingError.getMessage}")
          case Right(event)        => event
        }
    }
  }
}

object AggregateProcessingLambda {
  val logger: Logger = Logger[AggregateProcessingLambda]
  private val configFactory: Config = ConfigFactory.load()
  val s3Utils: S3Utils = S3Utils(S3Clients.s3Async(configFactory.getString("s3.endpoint")))

  private case class AggregateProcessingError(consignmentId: UUID, errorCode: String, errorMessage: String) extends BaseError {
    override def toString: String = {
      s"${this.simpleName}: consignmentId: $consignmentId, errorCode: $errorCode, errorMessage: $errorMessage"
    }
  }

  private case class AssetProcessingResult(errors: Boolean, suppliedMetadata: Boolean)
  case class AggregateEvent(metadataSourceBucket: String, metadataSourceObjectPrefix: String, dataLoadErrors: Boolean)
}
