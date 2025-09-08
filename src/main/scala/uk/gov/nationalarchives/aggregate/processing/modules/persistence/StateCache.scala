package uk.gov.nationalarchives.aggregate.processing.modules.persistence

import com.google.gson.Gson
import io.circe.{Json, ParsingFailure, parser}
import redis.clients.jedis.UnifiedJedis
import uk.gov.nationalarchives.aggregate.processing.modules.persistence.Model._
import uk.gov.nationalarchives.aggregate.processing.modules.persistence.StateCache.cacheClient

class StateCache {
  private val gson = new Gson

  implicit class JsonHelper(jsonBlob: AnyRef) {
    def toJsonString: String = {
      gson.toJson(jsonBlob)
    }
  }

  def setErrorState(state: ErrorState): Long = {
    val key = s"${state.consignmentId}:${TransferStateCategory.errorState}:${state.transferProcess}"
    val value = state.matchId
    cacheClient.sadd(key, value)
  }

  def getErrorState(errorStateFilter: ErrorStateFilter): Map[TransferProcess.Value, Long] = {
    val keyPrefix = s"${errorStateFilter.consignmentId}:${TransferStateCategory.errorState}"
    if (errorStateFilter.filter.isEmpty) {
      TransferProcess.values.map(v =>
        v -> cacheClient.scard(s"$keyPrefix:${v.toString}")
      ).toMap
    } else {
      errorStateFilter.filter.map(v => {
        v -> cacheClient.scard(s"$keyPrefix:${v.toString}")
      }).toMap
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

object StateCache {
  val cacheClient = new UnifiedJedis("redis://localhost:6379")
}
