package uk.gov.nationalarchives.aggregate.processing.modules.persistence

import com.google.gson.Gson
import redis.clients.jedis.UnifiedJedis
import uk.gov.nationalarchives.aggregate.processing.modules.persistence.Model.{PathState, TransferStateCategory}
import uk.gov.nationalarchives.aggregate.processing.modules.persistence.DataCache.cacheClient

import java.util.UUID

class DataCache {
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

  private def hSet(key: String, field: String, value: String): Long = {
    cacheClient.hset(key, field, value)
  }

  private def hGet(key: String, field: String): String = {
    cacheClient.hget(key, field)
  }
}

object DataCache {
  val cacheClient = new UnifiedJedis("redis://localhost:6379")
}
