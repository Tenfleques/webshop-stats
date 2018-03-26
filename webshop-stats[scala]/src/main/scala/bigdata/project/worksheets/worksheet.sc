import org.apache.spark._
import org.apache.spark.storage._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.dstream._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import java.util.HashMap
import java.nio.ByteBuffer

import org.apache.spark.streaming.receiver.Receiver

import scala.collection.mutable
import scala.util.Random


val clusterName = "localhost"
// === Configuration to control the flow of the application ===
val stopActiveContext = true
// "true"  = stop if any existing StreamingContext is running;
// "false" = dont stop, and let it run undisturbed, but your latest code may not be used
// === Configurations for Spark Streaming ===
val batchIntervalSeconds = 10
val eventsPerSecond = 1000    // For the dummy source
class DummySource(ratePerSec: Int) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) {

  def onStart() {
    // Start the thread that receives data over a connection
    new Thread("Dummy Source") {
      override def run() { receive() }
    }.start()
  }

  def onStop() {
    // There is nothing much to do as the thread calling receive()
    // is designed to stop by itself isStopped() returns false
  }

  /** Create a socket connection and receive data until receiver is stopped */
  private def receive() {
    while(!isStopped()) {
      store("I am a dummy source " + Random.nextInt(10))
      Thread.sleep((1000.toDouble / ratePerSec).toInt)
    }
  }
}

var newContextCreated = false      // Flag to detect whether new context was created or not
val kafkaBrokers = "localhost:2181"   // comma separated list of broker:host
// Function to create a new StreamingContext and set it up
def creatingFunc(): StreamingContext = {

  // Create a StreamingContext
  val sc = new SparkConf().setAppName("webshop-stats").setMaster("local[2]")
  val ssc = new StreamingContext(sc, Seconds(batchIntervalSeconds))

  // Create a stream that generates 1000 lines per second
  val stream = ssc.receiverStream(new DummySource(eventsPerSecond))

  // Split the lines into words, and then do word count
  val wordStream = stream.flatMap { _.split(" ")  }
  val wordCountStream = wordStream.map(word => (word, 1)).reduceByKey(_ + _)

  // Create temp table at every batch interval
  wordCountStream.foreachRDD { rdd =>
    rdd.toDF("word", "count").registerTempTable("batch_word_count")
  }

  wordCountStream.foreachRDD( rdd => {
    System.out.println("# events = " + rdd.count())

    rdd.foreachPartition( f = partition => {
      // Print statements in this section are shown in the executor's stdout logs
      val kafkaOpTopic = "clients-purchases"
      val props = new mutable.HashMap[String, Object]()
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers)
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer")
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer")

      val producer = new KafkaProducer[String, String](props)

      partition.foreach(record => {
        val data = record.toString
        // As as debugging technique, users can write to DBFS to verify that records are being written out
        // dbutils.fs.put("/tmp/test_kafka_output",data,true)
        val message = new ProducerRecord[String, String](kafkaOpTopic, null, data)
        producer.send(message)
      })
      producer.close()
    })

  })

  ssc.remember(Minutes(1))  // To make sure data is not deleted by the time we query it interactively

  println("Creating function called to create new StreamingContext")
  newContextCreated = true
  ssc
}

// Stop any existing StreamingContext
if (stopActiveContext) {
  StreamingContext.getActive.foreach { _.stop(stopSparkContext = false) }
}

val ssc = StreamingContext.getActiveOrCreate(creatingFunc)

ssc.start()
ssc.awaitTerminationOrTimeout(batchIntervalSeconds * 5 * 1000)


