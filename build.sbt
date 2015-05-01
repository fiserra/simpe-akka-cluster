name := """simple-akka-cluster"""

version := "1.0"

enablePlugins(JavaAppPackaging)

scalaVersion := "2.11.5"

val akkaVersion = "2.3.10"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-cluster" % akkaVersion,
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test")


