package bigdata.project.system

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._

class Pipe(brokers:String,topics: String){
  val sparkConf = new SparkConf().setAppName("referee-stats").setMaster("local[2]")
  val ssc = new StreamingContext(sparkConf, Seconds(5))

  val topicsSet = topics.split(",").toSet
  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> brokers,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "1",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )
  val messages = KafkaUtils.createDirectStream[String, String](
    ssc,
    LocationStrategies.PreferConsistent,
    ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))

  val streamView = messages.map(_.value)
  streamView.print()

  ssc.start()
  ssc.awaitTermination()
}


