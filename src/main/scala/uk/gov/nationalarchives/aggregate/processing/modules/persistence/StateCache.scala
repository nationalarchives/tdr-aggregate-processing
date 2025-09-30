package uk.gov.nationalarchives.aggregate.processing.modules.persistence


import redis.clients.jedis.UnifiedJedis
import uk.gov.nationalarchives.aggregate.processing.modules.persistence.Model.TransferStateCategory.{matchIdToFileIdState, pathToAssetState, pathToReferenceState}
import uk.gov.nationalarchives.aggregate.processing.modules.persistence.Model._
import uk.gov.nationalarchives.aggregate.processing.modules.persistence.StateCache.cacheClient

import java.util.UUID
import scala.jdk.CollectionConverters.CollectionHasAsScala

class StateCache {
  private val referencePoolKey = "reference:pool"

  private def psuedoReferenceGenerator(numberOfReferences: Long): List[String] = {
    val counterKey = "reference:counter"
    val currentCounter = cacheClient.get(counterKey).toLong
    cacheClient.incrBy(counterKey, numberOfReferences)
    (currentCounter until currentCounter + numberOfReferences).map(cc => s"ref-$cc").toList
  }

  def getReferences(numberOfReferences: Int): List[String] = {
    val remainingRefs = cacheClient.llen(referencePoolKey)
    if (remainingRefs < 500 + numberOfReferences) {
      replenishReferencePool(500)
    }
    cacheClient.rpop(referencePoolKey, numberOfReferences).asScala.toList
  }

  def updateTransferState(state: TransferState): Long = {
    val key = s"${state.consignmentId}:${state.transferState.toString}"
    setAdd(key, state.value)
  }

  def updateMatchIdToFileIdState(state: MatchIdToFileIdState): Long = {
    val key = s"${state.consignmentId}:$matchIdToFileIdState"
    val field = state.matchId
    val value = state.fileId
    hSetNx(key, field, value.toString)
  }

  def updatePathToAssetState(state: PathToAssetIdState): Long = {
    val key = s"${state.consignmentId}:$pathToAssetState"
    val field = state.path
    val value = state.assetIdentifier
    hSetNx(key, field, value)
  }

  def updatePathToReferenceState(state: PathToReferenceState): Long = {
    val key = s"${state.consignmentId}:$pathToReferenceState"
    val field = state.path
    val value = state.reference
    hSetNx(key, field, value)
  }

  def updateAssignedReferences(consignmentId: UUID, reference: String): Long = {
    val key = s"$consignmentId:assignedReferences"
    setAdd(key, reference)
  }

  def getAssetIdentifierByPath(consignmentId: UUID, path: String): String = {
    val key = s"$consignmentId:$pathToAssetState"
    hGet(key, path)
  }

  def getReferenceByPath(consignmentId: UUID, path: String): String = {
    val key = s"$consignmentId:$pathToReferenceState"
    hGet(key, path)
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

  def replenishReferencePool(numberOfReferences: Long): Unit = {
    val newRefs = psuedoReferenceGenerator(numberOfReferences)
    newRefs.foreach(cacheClient.lpush(referencePoolKey, _))
  }
}

object StateCache {
  val cacheClient = new UnifiedJedis("redis://localhost:6379")

  def apply() = new StateCache
}
