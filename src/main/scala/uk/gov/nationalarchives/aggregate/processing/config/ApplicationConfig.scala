package uk.gov.nationalarchives.aggregate.processing.config

import com.typesafe.config.{ConfigFactory, Config => TypeSafeConfig}
import uk.gov.nationalarchives.aws.utils.ssm.{SSMClients, SSMUtils}

import scala.concurrent.duration.{DurationInt, FiniteDuration}

object ApplicationConfig {
  private val configFactory: TypeSafeConfig = ConfigFactory.load

  private val ssmEndpoint: String = configFactory.getString("ssm.endpoint")
  private val authClientSecretPath: String = configFactory.getString("auth.clientSecretPath")

  val authClientId: String = configFactory.getString("auth.clientId")
  val authUrl: String = configFactory.getString("auth.url")
  val graphQlApiUrl: String = configFactory.getString("graphQlApi.url")
  val graphqlApiRequestTimeOut: FiniteDuration = 180.seconds
  val s3Endpoint: String = configFactory.getString("s3.endpoint")
  val atomicProcessing = configFactory.getString("featureAccessBlocks.atomicProcessing")
  val timeToLiveSecs: Int = 60

  def getClientSecret(secretPath: String = authClientSecretPath): String = {
    val ssmClient = SSMClients.ssm(ssmEndpoint)
    SSMUtils(ssmClient).getParameterValue(secretPath)
  }
}
