package uk.gov.nationalarchives.aggregate.processing.utilities

import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.{HeadObjectRequest, NoSuchKeyException}

import java.net.URI

class S3UtilsForTesting(client: S3AsyncClient) {
  def objectKeyExists(bucket: String, key: String): Boolean = {
    val headObjectRequest: HeadObjectRequest = HeadObjectRequest.builder()
      .bucket(bucket)
      .key(key)
      .build()

    try {
        client.headObject(headObjectRequest).get()
        true
      } catch {
        case e: Exception if e.getCause.isInstanceOf[NoSuchKeyException] =>
          false
      }
    }
}

object S3UtilsForTesting {
  def s3AsyncClient(): S3AsyncClient = {
    val httpClient = NettyNioAsyncHttpClient.builder.build
    S3AsyncClient.builder
      .region(Region.EU_WEST_2)
      .endpointOverride(URI.create("https://s3.eu-west-2.amazonaws.com"))
      .httpClient(httpClient)
      .build
  }

  def apply() = new S3UtilsForTesting(s3AsyncClient())
}
