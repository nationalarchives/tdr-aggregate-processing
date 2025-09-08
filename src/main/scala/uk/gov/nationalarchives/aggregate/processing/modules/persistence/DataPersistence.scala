package uk.gov.nationalarchives.aggregate.processing.modules.persistence

import com.google.gson.Gson
import io.circe.{Json, ParsingFailure, parser}
import redis.clients.jedis.UnifiedJedis
import uk.gov.nationalarchives.aggregate.processing.modules.persistence.Model.{AssetData, DataCategory}
import uk.gov.nationalarchives.aggregate.processing.modules.persistence.StateCache.cacheClient

class DataPersistence {
  private val gson = new Gson

  implicit class JsonHelper(jsonBlob: AnyRef) {
    def toJsonString: String = {
      gson.toJson(jsonBlob)
    }
  }

  def setAssetData(data: AssetData): Unit = {
    val assetIdentifier =  if (data.assetId.nonEmpty) {
      data.assetId.get.toString
    } else data.matchId

    val key = s"$assetIdentifier:${DataCategory.errorData.toString}"
    cacheClient.jsonSet(key, data.input)
  }

  def getAssetData(key: String): Either[ParsingFailure, Json] = {
    parser.parse(cacheClient.jsonGet(key).toJsonString)
  }

}

object DataPersistence {
  val dataPersistenceClient = new UnifiedJedis("redis://localhost:6379")
}
