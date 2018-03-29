package bigdata.project.system

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

class KafkaSink (createProducer: () => KafkaProducer[String, String]) extends Serializable {
  lazy val producer = createProducer()
  def send(topic: String, value: String): Unit = producer.send(new ProducerRecord(topic, value))
}

/*
* object KafkaSink {
    def apply(config: Map[String, Object]): KafkaSink = {
      val f = () => {
        val producer = new KafkaProducer[String, String](config)

        sys.addShutdownHook {
          producer.close()
        }
        producer
      }
      new KafkaSink(f)
    }
  }
* */