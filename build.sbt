import Dependencies._

ThisBuild / scalaVersion     := "2.13.17"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"

lazy val root = (project in file("."))
  .settings(
    name := "SparkConcepts",
    libraryDependencies += munit % Test
  )

libraryDependencies += "org.apache.spark" %% "spark-core" % "4.1.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "4.1.1"

ThisBuild / fork := true
ThisBuild / javaOptions += "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"



// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.
