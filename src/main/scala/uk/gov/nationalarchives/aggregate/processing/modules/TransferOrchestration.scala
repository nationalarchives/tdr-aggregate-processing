package uk.gov.nationalarchives.aggregate.processing.modules

import com.typesafe.scalalogging.Logger

import java.util.UUID

class TransferOrchestration {
  //Class to handle flow of consignment
  def orchestrate(consignmentId: UUID, transferState: TransferState): Unit = {
    /* TODO: Based on the transfer state:
    * - update Consignment statuses    *
    * - Trigger appropriate step functions
    * - Send appropriate notifications
    * */
  }
}

object TransferOrchestration {
  val logger: Logger = Logger[TransferOrchestration]
  def apply() = new TransferOrchestration()
}
