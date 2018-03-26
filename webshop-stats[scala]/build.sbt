name := "web-shop-stats"

version := "1.0"

scalaVersion := "2.11.5"
lazy val scalaTestVersion = "3.0.1"
lazy val sparkVersion = "2.3.0"

resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/",
  "Java.net Maven2 Repository" at "http://download.java.net/maven/2/"
)


libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
  "org.apache.spark" %% "spark-core" % sparkVersion % "compile",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "compile",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "compile",
  "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % sparkVersion % "compile"
)