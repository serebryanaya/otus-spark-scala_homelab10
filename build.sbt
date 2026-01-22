ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.18"

lazy val root = (project in file("."))
  .settings(
    name := "SparkTest"
  )

val sparkVersion = "3.5.5"
lazy val log4jVersion = "2.22.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "com.fasterxml.jackson.core" % "jackson-core" % "2.16.1",
  "org.scalatest" %% "scalatest" % "3.2.17" % "test",
  "com.github.mrpowers" %% "spark-fast-tests" % "1.3.0" % "test",
  "org.apache.logging.log4j" %% "log4j-api-scala" % "13.0.0",
  "org.apache.logging.log4j" % "log4j-api" % log4jVersion,
  "org.apache.logging.log4j" % "log4j-core" % log4jVersion
)
