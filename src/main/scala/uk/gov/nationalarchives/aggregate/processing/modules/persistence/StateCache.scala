package uk.gov.nationalarchives.aggregate.processing.modules.persistence


import redis.clients.jedis.UnifiedJedis
import uk.gov.nationalarchives.aggregate.processing.modules.persistence.DataPersistence.dataPersistenceClient
import uk.gov.nationalarchives.aggregate.processing.modules.persistence.Model.TransferStateCategory.{matchIdToFileIdState, pathToAssetState}
import uk.gov.nationalarchives.aggregate.processing.modules.persistence.Model._
import uk.gov.nationalarchives.aggregate.processing.modules.persistence.StateCache.cacheClient

import java.util.UUID

class StateCache {

  def createTransferState(state: TransferState): Long = {
    val key = s"${state.consignmentId}:${state.transferState.toString}"
    setAdd(key, state.value)
  }

  def createMatchIdToFileIdState(state: MatchIdToFileIdState): Long = {
    val key = s"${state.consignmentId}:$matchIdToFileIdState"
    val field = state.matchId
    val value = state.fileId
    hSetNx(key, field, value.toString)
  }

  def createPathToAssetState(state: PathToAssetIdState): Long = {
    val key = s"${state.consignmentId}:$pathToAssetState"
    val field = state.path
    val value = state.assetIdentifier
    hSetNx(key, field, value)
  }

  def getAssetIdentifierByPath(consignmentId: UUID, path: String): String = {
    val key = s"$consignmentId:$pathToAssetState"
    hGet(key, path)
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

  private def hSetNx(key: String, field: String, value: String): Long = {
    cacheClient.hsetnx(key, field, value)
  }

  private def hGet(key: String, field: String): String = {
    cacheClient.hget(key, field)
  }
}

object StateCache {
  val cacheClient = new UnifiedJedis("redis://localhost:6379")

  def apply() = new StateCache
}
