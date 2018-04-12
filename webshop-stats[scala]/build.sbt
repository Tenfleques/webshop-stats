name := "web-shop-stats"

version := "1.0"

scalaVersion := "2.11.0"
lazy val scalaTestVersion = "3.0.1"
lazy val sparkVersion = "2.3.0"
lazy val jettyVersion = "9.2.12.v20150709"
lazy val jerseyVersion = "2.19"

resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/",
  "Java.net Maven2 Repository" at "http://download.java.net/maven/2/"
)

libraryDependencies ++= Seq(
  //"org.scalatest" %% "scalatest" % scalaTestVersion % "test",
  "org.apache.spark" %% "spark-core" % sparkVersion % "compile",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "compile",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "compile",
  "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % sparkVersion % "compile",
  "org.apache.kafka" % "kafka-streams" % "1.1.0",
  "org.apache.kafka" % "kafka-clients" % "1.1.0",
  "org.slf4j" % "slf4j-api" % "1.7.10",
  "org.eclipse.jetty" % "jetty-server" % jettyVersion,
  "org.eclipse.jetty" % "jetty-servlet" % jettyVersion,
  "org.glassfish.jersey.media" % "jersey-media-json-jackson" % jerseyVersion

)