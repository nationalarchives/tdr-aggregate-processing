package uk.gov.nationalarchives.aggregate.processing.modules

import cats.effect.IO
import org.typelevel.log4cats.SelfAwareStructuredLogger

object ErrorHandling {
  trait BaseError {
    val simpleName: String = this.getClass.getSimpleName
  }

  def handleError(error: BaseError, logger: SelfAwareStructuredLogger[IO]): Unit = {
    // TODO: extend to write error JSON files to S3
    val errorMessage = error.toString
    logger.error(errorMessage)
  }
}
