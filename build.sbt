name := "SolrSupervisor"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "org.apache.curator" % "curator-recipes" % "2.8.0",
  "net.liftweb" %% "lift-json" % "2.6",
  "org.logback-extensions" % "logback-ext-loggly" % "0.1.2",
  //"com.amazonaws" % "aws-java-sdk" % "1.10.15",
  "ch.qos.logback" % "logback-classic" % "1.1.2",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",
  "com.typesafe.akka" %% "akka-actor" % "2.3.12"
)

mainClass in assembly := Some("solr.supervisor.Watch")
