package bigdata.project.system

import java.util.Properties

import com.typesafe.scalalogging.{LazyLogging, Logger}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory

class RefereeStats(brokers:String,topics: String) extends LazyLogging {
  val CHECKOUTDIR = "/tmp/refs"
  val REFERER_INDEX = 3
  def go(): StreamingContext = { //required 0 for count, 1 for value, 2 for purchases
    val conf = new SparkConf().setAppName("webshop-referee-stats").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))
    val logger = Logger(LoggerFactory.getLogger(this.getClass))

    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "streamer-xxx-",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    ssc.sparkContext.broadcast(brokers) //couldn't be found however inside the foreachRDD :-(

    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))

    val hitsCountPrepare = messages.map(record => {
      val row = record.value.split(",")
      (row(REFERER_INDEX),1L)
    })
    val hitsCount = hitsCountPrepare.reduceByKey(_+_)


    val hitsPurchasesPrepare = messages.map(record => {
      val row = record.value.split(",")
      //value = index5(quantity bought)
      (row(REFERER_INDEX),row(5).toLong)
    })
    val hitsPurchases = hitsPurchasesPrepare.reduceByKey(_+_);

    val hitsValuePrepare = messages.map(record => {
      val row = record.value.split(",")
      //value = index5(quantity bought) * index6(price of item)
      (row(REFERER_INDEX),row(5).toLong * row(6).toLong)
    })
    //hitsValuePrepare

    val hitsValue = hitsValuePrepare.reduceByKey(_+_)

    hitsValuePrepare.print()


    hitsValue.foreachRDD { rdd =>
      rdd.foreachPartition { partitionOfRecords =>
        val writerProps = new Properties
        writerProps.put("bootstrap.servers", "localhost:9092")
        //writerProps.put("client.id", "output-webshop-stats-referee-stats-xxx")
        writerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        writerProps.put("value.serializer", "org.apache.kafka.common.serialization.LongSerializer")

        val producer = new KafkaProducer[String, Long](writerProps)
        partitionOfRecords.foreach(record => {
          //println(record._1+"=> " + record._2.toString)
          producer.send(new ProducerRecord[String, Long]("output-webshop-stats-referee-stats-value",record._1,record
            ._2.toLong))
        })
        producer.close()
      }
    }

    hitsPurchases.foreachRDD { rdd =>
      rdd.foreachPartition { partitionOfRecords =>
        val writerProps = new Properties
        writerProps.put("bootstrap.servers", "localhost:9092")
        //writerProps.put("client.id", "output-webshop-stats-referee-stats-xxx")
        writerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        writerProps.put("value.serializer", "org.apache.kafka.common.serialization.LongSerializer")

        val producer = new KafkaProducer[String, Long](writerProps)
        partitionOfRecords.foreach(record => {
          producer.send(new ProducerRecord[String, Long]("output-webshop-stats-referee-purchases",record._1,
            record
              ._2.toLong))
        })
        producer.close()
      }
    }
    hitsCount.foreachRDD { rdd =>
      rdd.foreachPartition { partitionOfRecords =>
        //writeToKafka(partitionOfRecords,"output-webshop-stats-referee-count") raise null pointer exception
        val writerProps = new Properties
        writerProps.put("bootstrap.servers", "localhost:9092")
        //writerProps.put("client.id", "output-webshop-stats-referee-stats-xxx")
        writerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        writerProps.put("value.serializer", "org.apache.kafka.common.serialization.LongSerializer")

        val producer = new KafkaProducer[String, Long](writerProps)
        partitionOfRecords.foreach(record => {
          producer.send(new ProducerRecord[String, Long]("output-webshop-stats-referee-count",record._1,
            record
              ._2.toLong))
        })
        producer.close()
      }
    }
    ssc.checkpoint(CHECKOUTDIR)
    ssc
  }
  val context = StreamingContext.getOrCreate(CHECKOUTDIR, go _)

  context.start()
  context.awaitTermination()

}

