package uk.gov.nationalarchives.aggregate.processing.modules.persistence


import redis.clients.jedis.UnifiedJedis
import uk.gov.nationalarchives.aggregate.processing.modules.persistence.Model._
import uk.gov.nationalarchives.aggregate.processing.modules.persistence.StateCache.cacheClient

class StateCache {
  def setTransferState(state: TransferState): Long = {
    val key = s"${state.consignmentId}:${state.transferState}"
    setAdd(key, state.value.toString)
  }

  def setErrorState(errorState: ErrorState): Long = {
    val key = s"${errorState.consignmentId}:${TransferStateCategory.errorState}:${errorState.transferProcess}"
    val value = errorState.matchId
    setAdd(key, value)
  }

  def getTransferState(transferStateFilter: TransferStateFilter): Map[String, Long] = {
    val keyPrefix = s"${transferStateFilter.consignmentId}"
    val filters: Set[String] = if (transferStateFilter.filter.nonEmpty) {
      transferStateFilter.filter.map(_.toString)
    } else TransferStateCategory.values.map(_.toString)
    getState(keyPrefix, filters)
  }

  def getErrorState(errorStateFilter: ErrorStateFilter): Map[String, Long] = {
    val keyPrefix = s"${errorStateFilter.consignmentId}:${TransferStateCategory.errorState}"
    val filters: Set[String] = if (errorStateFilter.filter.nonEmpty) {
      errorStateFilter.filter.map(_.toString)
    } else TransferProcess.values.map(_.toString)
    getState(keyPrefix, filters)
  }

  private def getState(keyPrefix: String, filters: Set[String]): Map[String, Long] = {
    filters.map(v => {
      v -> getSetCardinality(s"$keyPrefix:$v")
    }).toMap
  }

  private def getSetCardinality(key: String): Long = {
    cacheClient.scard(key)
  }

  private def setAdd(key: String, value: String): Long = {
    cacheClient.sadd(key, value)
  }
}

object StateCache {
  val cacheClient = new UnifiedJedis("redis://localhost:6379")
}
