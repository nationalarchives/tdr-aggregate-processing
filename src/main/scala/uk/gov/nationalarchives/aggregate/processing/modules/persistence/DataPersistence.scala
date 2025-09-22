package uk.gov.nationalarchives.aggregate.processing.modules.persistence

import com.google.gson.Gson
import io.circe.{Json, ParsingFailure, parser}
import redis.clients.jedis.UnifiedJedis
import uk.gov.nationalarchives.aggregate.processing.modules.persistence.DataCache.cacheClient
import uk.gov.nationalarchives.aggregate.processing.modules.persistence.DataPersistence.dataPersistenceClient
import uk.gov.nationalarchives.aggregate.processing.modules.persistence.Model.{AssetData, DataCategory, PathState, TransferStateCategory}
import uk.gov.nationalarchives.aggregate.processing.modules.persistence.StateCache.cacheClient

import java.util.UUID

class DataPersistence {
  implicit class JsonHelper(jsonBlob: AnyRef) {
    private val gson = new Gson

    def toJsonString: String = {
      gson.toJson(jsonBlob)
    }
  }

  def setPathToAssetData(pathState: PathState): Long = {
    val key = s"${pathState.consignmentId}:${TransferStateCategory.pathsState}"
    val field = pathState.path
    val value = pathState.assetIdentifier
    hSet(key, field, value)
  }

  def getAssetIdentifierByPath(consignmentId: UUID, path: String): String = {
    val key = s"$consignmentId:${TransferStateCategory.pathsState}"
    hGet(key, path)
  }

  def setAssetData(data: AssetData): Unit = {
    val assetIdentifier =  if (data.assetId.nonEmpty) {
      data.assetId.get.toString
    } else data.matchId

    val key = s"$assetIdentifier:${DataCategory.errorData.toString}"
    dataPersistenceClient.jsonSet(key, data.input)
  }

  def getAssetData(key: String): Either[ParsingFailure, Json] = {
    parser.parse(dataPersistenceClient.jsonGet(key).toJsonString)
  }

  private def hSet(key: String, field: String, value: String): Long = {
    dataPersistenceClient.hset(key, field, value)
  }

  private def hGet(key: String, field: String): String = {
    dataPersistenceClient.hget(key, field)
  }
}

object DataPersistence {
  val dataPersistenceClient = new UnifiedJedis("redis://localhost:6379")
}
