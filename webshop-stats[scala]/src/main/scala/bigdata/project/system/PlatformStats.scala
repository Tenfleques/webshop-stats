package bigdata.project.system

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext, kafka010}
import org.apache.spark.{SparkConf, TaskContext}

class PlatformStats(brokers:String,topics: String) {
  val conf = new SparkConf().setAppName("referee-stats").setMaster("local[2]")
  val ssc = new StreamingContext(conf, Seconds(5))

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


   messages.map(_.value)

  val offsetRanges = Array (
    kafka010.OffsetRange(topics,0,0,100),
    kafka010.OffsetRange(topics,1,0,100)
  )

  /*val rdd = KafkaUtils.createRDD[String, String](sc, kafkaParams, offsetRanges, LocationStrategies
    .PreferConsistent)*/

  messages.foreachRDD(rdd => {
    val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    rdd.foreachPartition(iter => {
      val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
      println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
    })
  })


  ssc.start()
  ssc.awaitTermination()
}

