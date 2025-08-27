package uk.gov.nationalarchives.aggregate.processing.modules

import com.typesafe.scalalogging.Logger

object ErrorHandling {
  trait BaseError {}

  def handleError(error: BaseError, logger: Logger): Unit = {
    // TODO: extend to write error JSON files to S3
    val errorMessage = error.toString
    logger.error(errorMessage)
  }
}
