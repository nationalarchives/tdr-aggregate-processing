package uk.gov.nationalarchives.aggregate.processing.modules.persistence

import com.google.gson.Gson
import io.circe.{Json, ParsingFailure, parser}
import redis.clients.jedis.UnifiedJedis
import redis.clients.jedis.json.Path2
import uk.gov.nationalarchives.aggregate.processing.modules.persistence.DataPersistence.dataPersistenceClient
import uk.gov.nationalarchives.aggregate.processing.modules.persistence.Model.TransferStateCategory.errorState
import uk.gov.nationalarchives.aggregate.processing.modules.persistence.Model.{AssetData, ErrorData}

class DataPersistence(client: UnifiedJedis) {
  implicit class JsonHelper(jsonBlob: AnyRef) {
    private val gson = new Gson

    def toJsonString: String = {
      gson.toJson(jsonBlob)
    }
  }

  def setErrorData(data: ErrorData): Unit = {
    val consignmentId = data.consignmentId
    val identifier = data.objectIdentifier
    val key = s"$consignmentId:$errorState:${data.DataCategory.toString}:$identifier"
    dataPersistenceClient.jsonSet(key, data.input)
  }

  def setAssetData(data: AssetData): Unit = {
    val consignmentId = data.consignmentId
    val assetIdentifier = data.assetId
    val dataCategory = data.category.toString
    val pathTo = data.pathTo.getOrElse(Path2.ROOT_PATH)

    val key = s"$consignmentId:$dataCategory:$assetIdentifier"
    dataPersistenceClient.jsonSet(key, pathTo, data.input)
  }

  def getData(key: String, pathTo: Option[Path2] = None): Either[ParsingFailure, Json] = {
    val path = pathTo.getOrElse(Path2.ROOT_PATH)
    parser.parse(dataPersistenceClient.jsonGet(key, path).toJsonString)
  }
}

object DataPersistence {
  val dataPersistenceClient: UnifiedJedis = new UnifiedJedis("redis://localhost:6379")

  def apply() = new DataPersistence(dataPersistenceClient)
}
