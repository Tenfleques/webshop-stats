package bigdata.project.system

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

class DateIncidenceStats(brokers:String,topics: String) {
  val TIMESTAMP_INDEX = 1
  val conf = new SparkConf().setAppName("referee-stats-x").setMaster("local[2]")
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

  //messages.map(_.value.split(",")(ITEM_INDEX)).persist() using a constant index returns Task not serializable

  val hitsCountPrep = messages.map(record => {
    val row = record.value.split(",")
    (row(1),1L)
  })
  val hitsCount = hitsCountPrep.reduceByKey(_+_)

  val hitsPurchasesPrep = messages.map(record => {
    val row = record.value.split(",")
    //value = index5(quantity bought)
    (row(1),row(5).toLong)
  })

  val hitsPurchases = hitsPurchasesPrep.reduceByKey(_+_)

  val hitsValuePrep = messages.map(record => {
    val row = record.value.split(",")
    //value = index5(quantity bought) * index6(price of item)
    (row(1),row(5).toLong * row(6).toLong)
  })

  val hitsValue = hitsValuePrep.reduceByKey(_+_)

  //hitsCount.print()
  //hitsValue.print()
  hitsValue.print()
  hitsPurchases
    .foreachRDD { rdd =>
      rdd.foreachPartition { partitionOfRecords =>
      /*val connection = createNewConnection()
      partitionOfRecords.foreach(record => connection.send(record))
      connection.close()*/
    }
  }
  hitsPurchases.foreachRDD(rdd => {

  })

  //hitsPurchases.print()

  ssc.start()
  ssc.awaitTermination()
}
