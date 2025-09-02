package uk.gov.nationalarchives.aggregate.processing.utilities

import com.typesafe.scalalogging.Logger
import uk.gov.nationalarchives.utf8.validator.{ValidationException, ValidationHandler}

class UTF8ValidationHandler()(implicit logger: Logger) extends ValidationHandler {
  override def error(message: String, byteOffset: Long): Unit = {
    logger.error("[Error][@" + byteOffset + "] " + message)
    throw new ValidationException(message, byteOffset)
  }
}
