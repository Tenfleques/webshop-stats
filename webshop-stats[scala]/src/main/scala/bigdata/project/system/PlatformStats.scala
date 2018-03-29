package bigdata.project.system

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext, kafka010}

class PlatformStats(brokers:String,topics: String) {
  val PLATFORM_INDEX = 2
  val conf = new SparkConf().setAppName("referee-stats").setMaster("local[2]")
  val ssc = new StreamingContext(conf, Seconds(5))

  val topicsSet = topics.split(",").toSet
  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> brokers,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "1",
    "auto.offset.reset" -> "earliest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  val messages = KafkaUtils.createDirectStream[String, String](
    ssc,
    LocationStrategies.PreferConsistent,
    ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))


  val offsetRanges = Array (
    kafka010.OffsetRange(topics,0,0,100),
    kafka010.OffsetRange(topics,1,0,100)
  )

  //messages.map(_.value.split(",")(PLATFORM_INDEX)).persist() using a constant index returns Task not serializable

  val hitsCount = messages.map(record => {
    val row = record.value.split(",")
    (row(2),1L)
  }).reduce((x,y) => {
    (x._1, x._2 + y._2)
  })
  //can't print after reduce, reduceByKey don't know why all the methods get the java.lang.NoSuchMethodError: exception

  val hitsPurchases = messages.map(record => {
    val row = record.value.split(",")
    //value = index5(quantity bought)
    (row(2),row(5).toLong)
  })

  val hitsValue = messages.map(record => {
    val row = record.value.split(",")
    //value = index5(quantity bought) * index6(price of item)
    (row(2),row(5).toLong * row(6).toLong)
  })

  hitsPurchases.persist().print()



  /*messages.foreachRDD(rdd => {
    val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    rdd.foreachPartition(iter => {
      val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)

      if(!iter.isEmpty){
        iter.foreach(consumerRecord => {
          val record = consumerRecord.value().split(",")
        })
      }
      println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
    })
  })*/


  ssc.start()
  ssc.awaitTermination()
}

