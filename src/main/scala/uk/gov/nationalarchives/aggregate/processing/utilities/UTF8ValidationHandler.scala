package uk.gov.nationalarchives.aggregate.processing.utilities

import uk.gov.nationalarchives.utf8.validator.{ValidationException, ValidationHandler}

class UTF8ValidationHandler() extends ValidationHandler {
  override def error(message: String, byteOffset: Long): Unit = {
    throw new ValidationException(message, byteOffset)
  }
}
