package bigdata.project.system
import java.util.Properties

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.spark.rdd.RDD

class KafkaSink(brokers : String) {
  val props = new Properties()
  props.put("bootstrap.servers", brokers)
  props.put("acks", "all")
  //props.put("retries", 0)
  //props.put("batch.size", 16384)
  //props.put("linger.ms", 1)
  //props.put("buffer.memory", 33554432)
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  val producer = new KafkaProducer[String,String](props)
  def writeToKafka(rdd : RDD [(String,Long)]): Unit ={
    val  props = new Properties()
    props.put("bootstrap.servers","localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)
    rdd.foreach(record => {
      print(record._1+"=>"+record._2.toString)
      //producer.send(new ProducerRecord[String, String]("refree-stats-out",record._1,record._2.toString))
    })
  }
  def done(): Unit ={
    producer.close()
  }
}
