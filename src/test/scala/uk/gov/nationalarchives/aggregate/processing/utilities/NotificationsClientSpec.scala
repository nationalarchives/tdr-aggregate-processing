package uk.gov.nationalarchives.aggregate.processing.utilities

import cats.effect.unsafe.implicits.global
import com.typesafe.config.ConfigFactory
import io.circe.generic.auto._
import io.circe.syntax.EncoderOps
import org.mockito.ArgumentMatchers._
import org.mockito.{ArgumentCaptor, MockitoSugar}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.services.sns.model.PublishResponse
import uk.gov.nationalarchives.aws.utils.sns.SNSUtils

class NotificationsClientSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  "publishUploadEvent" should "publish the event and return a PublishResponse" in {
    val snsUtils = mock[SNSUtils]
    val config = ConfigFactory.load()
    val topicArn = config.getString("sns.notificationsTopicArn")
    val uploadEvent = NotificationsClient.UploadEvent("body", "ref", "consignmentId", "status", "userId", "userEmail", "uploadSource", "environment")
    val publishResponse = PublishResponse.builder().messageId("123").build()

    when(snsUtils.publish(any[String], any[String])).thenReturn(publishResponse)

    val client = new NotificationsClient(snsUtils, config)
    client.publishUploadEvent(uploadEvent).unsafeRunSync()

    val messageCaptor = ArgumentCaptor.forClass(classOf[String])
    val topicCaptor = ArgumentCaptor.forClass(classOf[String])
    verify(snsUtils).publish(messageCaptor.capture(), topicCaptor.capture())
    messageCaptor.getValue shouldBe uploadEvent.asJson.toString()
    topicCaptor.getValue shouldBe topicArn
  }

  "publishUploadEvent" should "fail when snsUtils.publish throws an exception" in {
    val snsUtils = mock[SNSUtils]
    val config = ConfigFactory.load()
    val uploadEvent = NotificationsClient.UploadEvent("body", "ref", "consignmentId", "status", "userId", "userEmail", "uploadSource", "environment")
    val exception = new RuntimeException("SNS publish failed")
    when(snsUtils.publish(any[String], any[String])).thenThrow(exception)

    val client = new NotificationsClient(snsUtils, config)
    val result = client.publishUploadEvent(uploadEvent).attempt.unsafeRunSync()

    result.left.getOrElse("") shouldBe exception
  }
}
