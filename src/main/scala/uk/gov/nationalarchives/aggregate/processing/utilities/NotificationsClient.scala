package uk.gov.nationalarchives.aggregate.processing.utilities

import cats.effect.IO
import com.typesafe.config.Config
import io.circe.generic.auto._
import io.circe.syntax.EncoderOps
import software.amazon.awssdk.services.sns.model.PublishResponse
import uk.gov.nationalarchives.aggregate.processing.utilities.NotificationsClient.UploadEvent
import uk.gov.nationalarchives.aws.utils.sns.SNSClients.sns
import uk.gov.nationalarchives.aws.utils.sns.SNSUtils

class NotificationsClient(snsUtils: SNSUtils, config: Config) {

  def publishUploadEvent(uploadEvent: UploadEvent): IO[PublishResponse] =
    IO(snsUtils.publish(uploadEvent.asJson.toString(), config.getString("sns.notificationsTopicArn")))
}

object NotificationsClient {

  def apply(config: Config): NotificationsClient = new NotificationsClient(SNSUtils(sns(config.getString("sns.endpoint"))), config)

  case class UploadEvent(
      transferringBodyName: String,
      consignmentReference: String,
      consignmentId: String,
      status: String,
      userId: String,
      userEmail: String
  )
}
