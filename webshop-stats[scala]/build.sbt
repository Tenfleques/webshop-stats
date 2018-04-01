name := "web-shop-stats"

version := "1.0"

scalaVersion := "2.12.0"
lazy val scalaTestVersion = "3.0.1"
lazy val sparkVersion = "2.1.0"

resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/",
  "Java.net Maven2 Repository" at "http://download.java.net/maven/2/"
)

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.8.7"

libraryDependencies ++= Seq(
  //"org.scalatest" %% "scalatest" % scalaTestVersion % "test",
  "org.apache.spark" %% "spark-core" % sparkVersion % "compile",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "compile",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "compile",
  "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % sparkVersion % "compile",
  "org.apache.kafka" % "kafka-streams" % "1.1.0"
)