name := "kafka-tutorials"
organization := "com.ippontech"
version := "0.0.1-SNAPSHOT"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.0"

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-library" % scalaVersion.value,

  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion
  
)
libraryDependencies += "org.datasyslab" % "geospark" % "1.2.0" % "provided"
