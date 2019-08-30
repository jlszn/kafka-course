name := "kafka-beginners-course"

version := "0.1"

scalaVersion := "2.13.0"

val kafkaVersion = "2.3.0"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % kafkaVersion,
  "org.slf4j" % "slf4j-simple" % "1.7.28"
)