import sbt.*

object Dependencies {
  private val circeVersion = "0.14.14"
  private val mockitoScalaVersion = "2.0.0"

  lazy val awsLambdaCore = "com.amazonaws" % "aws-lambda-java-core" % "1.3.0"
  lazy val awsLambdaEvents = "com.amazonaws" % "aws-lambda-java-events" % "3.16.1"
  lazy val awsSqs = "com.amazonaws" % "aws-java-sdk-sqs" % "1.12.788"

  lazy val circeCore = "io.circe" %% "circe-core" % circeVersion
  lazy val circeGeneric = "io.circe" %% "circe-generic" % circeVersion
  lazy val circeParser = "io.circe" %% "circe-parser" % circeVersion

  lazy val gson = "com.google.code.gson" % "gson" % "2.13.1"

  lazy val jedis = "redis.clients" % "jedis" % "6.2.0"

  lazy val logback = "ch.qos.logback" % "logback-classic" % "1.5.18"
  lazy val logstash = "net.logstash.logback" % "logstash-logback-encoder" % "8.1"

  lazy val mockitoScala = "org.mockito" %% "mockito-scala" % mockitoScalaVersion
  lazy val mockitoScalaTest = "org.mockito" %% "mockito-scala-scalatest" % mockitoScalaVersion

  lazy val s3Utils = "uk.gov.nationalarchives" %% "s3-utils" % "0.1.294"
  lazy val scalaLogging = "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5"
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.2.19"

  lazy val typesafeConfig = "com.typesafe" % "config" % "1.4.4"

  lazy val utf8Validator = "uk.gov.nationalarchives" % "utf8-validator" % "1.2"

  lazy val wiremock = "com.github.tomakehurst" % "wiremock" % "3.0.1"
}
