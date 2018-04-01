package bigdata.project.system
import java.util.Properties

import org.apache.kafka.clients.producer.KafkaProducer

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

  def write(topic :String, value: String): Unit = {
    /*producer.send(new ProducerRecord[String, String](topic, value)) {
    }*/
  }
  def done(): Unit ={
    producer.close()
  }



}
