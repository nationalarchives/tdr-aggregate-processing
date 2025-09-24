import sbt.*

object Dependencies {
  private val circeVersion = "0.14.14"
  private val log4CatsVersion = "2.7.1"
  private val mockitoScalaVersion = "2.0.0"

  lazy val authUtils = "uk.gov.nationalarchives" %% "tdr-auth-utils" % "0.0.259"
  lazy val awsLambdaCore = "com.amazonaws" % "aws-lambda-java-core" % "1.4.0"
  lazy val awsLambdaEvents = "com.amazonaws" % "aws-lambda-java-events" % "3.16.1"
  lazy val awsSqs = "com.amazonaws" % "aws-java-sdk-sqs" % "1.12.791"

  lazy val circeCore = "io.circe" %% "circe-core" % circeVersion
  lazy val circeGeneric = "io.circe" %% "circe-generic" % circeVersion
  lazy val circeParser = "io.circe" %% "circe-parser" % circeVersion

  lazy val generatedGraphql = "uk.gov.nationalarchives" %% "tdr-generated-graphql" % "0.0.433"
  lazy val graphqlClient = "uk.gov.nationalarchives" %% "tdr-graphql-client" % "0.0.253"

  lazy val logback = "ch.qos.logback" % "logback-classic" % "1.5.18"
  lazy val logstash = "net.logstash.logback" % "logstash-logback-encoder" % "8.1"

  lazy val log4cats = "org.typelevel" %% "log4cats-core" % log4CatsVersion
  lazy val log4catsSlf4j = "org.typelevel" %% "log4cats-slf4j" % log4CatsVersion

  lazy val mockitoScala = "org.mockito" %% "mockito-scala" % mockitoScalaVersion
  lazy val mockitoScalaTest = "org.mockito" %% "mockito-scala-scalatest" % mockitoScalaVersion

  lazy val s3Utils = "uk.gov.nationalarchives" %% "s3-utils" % "0.1.304"
  lazy val scalaLogging = "com.typesafe.scala-logging" %% "scala-logging" % "3.9.6"
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.2.19"
  lazy val slf4j = "org.slf4j" % "slf4j-simple" % "2.0.17"
  lazy val ssmUtils = "uk.gov.nationalarchives" %% "ssm-utils" % "0.1.304"
  lazy val stepFunctionUtils = "uk.gov.nationalarchives" %% "stepfunction-utils" % "0.1.304"

  lazy val typesafeConfig = "com.typesafe" % "config" % "1.4.5"

  lazy val utf8Validator = "uk.gov.nationalarchives" % "utf8-validator" % "1.2"

  lazy val wiremock = "com.github.tomakehurst" % "wiremock" % "3.0.1"
}
