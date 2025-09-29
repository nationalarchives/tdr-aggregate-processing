import Dependencies._
import sbt.Keys.fork

ThisBuild / scalaVersion := "2.13.16"
ThisBuild / version := "0.1.0"
ThisBuild / organization := "uk.gov.nationalarchives"
ThisBuild / organizationName := "aggregate-processing"

libraryDependencies ++= Seq(
  authUtils,
  awsLambdaCore,
  awsLambdaEvents,
  awsSqs,
  circeCore,
  circeGeneric,
  circeParser,
  generatedGraphql,
  graphqlClient,
  logback,
  logstash,
  mockitoScala % Test,
  mockitoScalaTest % Test,
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
