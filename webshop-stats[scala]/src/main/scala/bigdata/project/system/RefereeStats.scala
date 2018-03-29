package bigdata.project.system

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

class RefereeStats(brokers:String,topics: String) {
  val REFERER_INDEX = 3
  val CHECKOUTDIR = "/tmp/webshop-referer-streams"


  def runStats(): StreamingContext = {
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

    //messages.map(_.value.split(",")(REFERER_INDEX)).persist() using a constant index returns Task not serializable

    val hitsCountPrepare = messages.map(record => {
      val row = record.value.split(",")
      (row(3),1L)
    })
    val hitsCount = hitsCountPrepare.reduce((x,y) => {
      (x._1, x._2 + y._2)
    })
    val hitsPurchasesPrepare = messages.map(record => {
      val row = record.value.split(",")
      //value = index5(quantity bought)
      (row(3),row(5).toLong)
    })
    val hitsPurchases = hitsPurchasesPrepare.reduceByKey(_+_);

    val hitsValuePrepare = messages.map(record => {
      val row = record.value.split(",")
      //value = index5(quantity bought) * index6(price of item)
      (row(3),row(5).toLong * row(6).toLong)
    })
    val hitsValue = hitsValuePrepare.reduceByKey(_+_)

    hitsCount.print()
    hitsValue.print()
    hitsPurchases.print()

    //@TODO
    //write results to kafka for consuption by nodejs client application
    /*hitsCount.foreachRDD { rdd =>
      rdd.foreachPartition { partitionOfRecords =>
        val producer = createKafkaProducer()
        partitionOfRecords.foreach { message =>
          connection.send(message)
        }
        producer.close()
      }
    }*/



    ssc.checkpoint(CHECKOUTDIR)
    ssc
  }

  val context = StreamingContext.getOrCreate(CHECKOUTDIR, runStats _)

  context.start()
  context.awaitTermination()
}

