import Dependencies._
import sbt.Keys.fork

ThisBuild / scalaVersion := "2.13.18"
ThisBuild / version := "0.1.0"
ThisBuild / organization := "uk.gov.nationalarchives"
ThisBuild / organizationName := "aggregate-processing"

libraryDependencies ++= Seq(
  authUtils,
  awsLambdaCore,
  awsLambdaEvents,
  circeCore,
  circeGeneric,
  circeParser,
  csvParser,
  generatedGraphql,
  graphqlClient,
  logback,
  logstash,
  metadataSchema,
  mockitoScala % Test,
  mockitoScalaTest % Test,
  parallelCollections,
  s3Utils,
  scalaLogging,
  scalaTest % Test,
  snsUtils,
  ssmUtils,
  stepFunctionUtils,
  typesafeConfig,
  utf8Validator,
  wiremock % Test
)

excludeDependencies ++= Seq(
  //Remove transitory dependencies to reduce overall jar size to allow deployment as lambda
  ExclusionRule("com.fasterxml.jackson.dataformat"),
  ExclusionRule("org.keycloak", "keycloak-server-spi"),
  ExclusionRule("org.keycloak", "keycloak-server-spi-private"),
  ExclusionRule("org.keycloak", "keycloak-crypto-default"),
  ExclusionRule("com.softwaremill")
)

(Test / fork) := true
(Test / javaOptions) += s"-Dconfig.file=${sourceDirectory.value}/test/resources/application.conf"
(Test / envVars) := Map("AWS_ACCESS_KEY_ID" -> "test", "AWS_SECRET_ACCESS_KEY" -> "test")

(assembly / assemblyMergeStrategy) := {
  case PathList("META-INF", "MANIFEST.MF")       => MergeStrategy.discard
  case PathList("META-INF", "services", xs @ _*) => MergeStrategy.concat
  case PathList("META-INF", xs @ _*)             => MergeStrategy.discard
  case _                                         => MergeStrategy.first
}
(assembly / assemblyJarName) := "aggregate-processing.jar"
