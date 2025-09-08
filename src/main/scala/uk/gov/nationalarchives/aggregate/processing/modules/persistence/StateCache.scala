package uk.gov.nationalarchives.aggregate.processing.modules.persistence

import com.google.gson.Gson
import io.circe.{Json, ParsingFailure, parser}
import redis.clients.jedis.UnifiedJedis
import uk.gov.nationalarchives.aggregate.processing.modules.persistence.Model._
import uk.gov.nationalarchives.aggregate.processing.modules.persistence.StateCache.cacheClient

import java.util.UUID

class StateCache {
  private val gson = new Gson

  implicit class JsonHelper(jsonBlob: AnyRef) {
    def toJsonString: String = {
      gson.toJson(jsonBlob)
    }
  }

  def setPathState(pathState: PathState): Long = {
    val key = s"${pathState.consignmentId}:${TransferStateCategory.pathsState}"
    val value = pathState.path
    setAdd(key, value)
  }

  def getPathState(consignmentId: UUID): Long = {
    getSetCardinality(consignmentId.toString)
  }

  def getPaths(consignmentId: UUID): Set[String] = {

  }

  def setErrorState(errorState: ErrorState): Long = {
    val key = s"${errorState.consignmentId}:${TransferStateCategory.errorState}:${errorState.transferProcess}"
    val value = errorState.matchId
    setAdd(key, value)
  }

  private def setAdd(key: String, value: String): Long = {
    cacheClient.sadd(key, value)
  }

  def getErrorState(errorStateFilter: ErrorStateFilter): Map[TransferProcess.Value, Long] = {
    val keyPrefix = s"${errorStateFilter.consignmentId}:${TransferStateCategory.errorState}"
    if (errorStateFilter.filter.isEmpty) {
      TransferProcess.values.map(v =>
        v -> getSetCardinality(s"$keyPrefix:${v.toString}")
      ).toMap
    } else {
      errorStateFilter.filter.map(v => {
        v -> getSetCardinality(s"$keyPrefix:${v.toString}")
      }).toMap
    }
  }

  private def getSetCardinality(key: String): Long = {
    cacheClient.scard(key)
  }

  private def scanSet(key: String): Set[String] = {
    cacheClient.scanIteration()
    Set()
  }
}

object StateCache {
  val cacheClient = new UnifiedJedis("redis://localhost:6379")
}
