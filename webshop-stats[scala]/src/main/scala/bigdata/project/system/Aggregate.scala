package bigdata.project.system

import java.io.File
import java.nio.file.Files
import java.util.{Date, Properties}
import java.util.concurrent.CountDownLatch

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream.{KGroupedStream, KStream, Materialized, Serialized}
import org.apache.kafka.streams.processor.WallclockTimestampExtractor
import org.apache.kafka.streams.state.KeyValueStore


/*
* All the methods and fields are private save for the constructor so that all statistic calls are controlled and
* fixed to one for each instance of the class
*/

//19,1499882547,Platform e.g Android 4.0.2,instagram.*,HTC Desire HD,0,5000
object Aggregate {
  @throws[Exception]
  private[project] def startRestProxy(streams: KafkaStreams, port: Int): RPCService = {
    val aggregateRestService: RPCService = new RPCService(streams)
    aggregateRestService.start(port)
    return aggregateRestService
  }
}

class Aggregate @throws[Exception]
(val KEY_FIELD: Integer // the statistic of count, purchase count and purchase value is made against me
 , val brokers: String, val topic: String, val statistic: Int, val rpcEndpoint: String, val rpcPort: Integer) {
  //statistic
  // restricted to 0-2
  // count, purchase count and purchase value respectively
  // endpoint exposes the app info to the world
  private var storeName: String = ""
  final private var props: Properties  = new Properties
  val APP_ID: String = "stream-aggregate-data" + new Date().hashCode
  props.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID)
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
  props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, rpcEndpoint + ":" + rpcPort)
  props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass)
  props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass)
  props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, classOf[WallclockTimestampExtractor])
  val storeFile: File = Files.createTempDirectory(new File("/tmp").toPath, APP_ID).toFile
  props.put(StreamsConfig.STATE_DIR_CONFIG, storeFile.getPath)
  var streams: KafkaStreams = null
  statistic match {
    case 2 =>
      streams = runPurchaseValueStat
    case 1 =>
      streams = runPurchaseCountStat
    case _ =>
      streams = runCountStat
  }
  //expose the available REST endpoints
  val info: String = "\n" + "* available endpoints :\n" + "*\t     http://" + rpcEndpoint + ":" + rpcPort + "/stats/" + storeName + "/all\n" + //TODO create platform to query the state of the given list
    //"*\t     http://"+rpcEndpoint+":"+rpcPort+"/"+storeName+"/{listofkeys}\n" +
    "*\t     http://" + rpcEndpoint + ":" + rpcPort + "/instances\n" + "*\t     http://" + rpcEndpoint + ":" + rpcPort + "/instances/" + storeName + "\n" + "*\t     http://" + rpcEndpoint + ":" + rpcPort + "/instance/" + storeName + "/{key}\n\n"
  System.out.print(info)
  streams.start()
  val restService: RPCService = Aggregate.startRestProxy(streams, rpcPort)
  val latch: CountDownLatch = new CountDownLatch(1)
  try {
    latch.await()
  } catch {
    case e: Throwable =>
      System.exit(1)
  }
  Runtime.getRuntime.addShutdownHook(new Thread(("streams-shutdown-hook")) {
    override def run(): Unit = {
      streams.close()
      try {
        //restService.stop
      } catch {
        case e: Exception =>
          System.out.print(e.getMessage)
      }
      latch.countDown()
    }
  })
  System.exit(0)

  private def runPurchaseValueStat: KafkaStreams = {
    val builder: StreamsBuilder = new StreamsBuilder
    val sourceHits: KStream[String, String] = builder.stream(this.topic)
    val refererHitsCount: KGroupedStream[String, String] = sourceHits.mapValues((value: String) => new WebRecord(value)
      .getPurchasesCount(this.KEY_FIELD)).map((key: String, keyValue: KeyValue[String,String]) => keyValue)
      .groupBy((key: String, value: String) => key, Serialized.`with`(Serdes.String, Serdes.String))

    this.storeName = "value"
    val store: Materialized[String,String, KeyValueStore[Bytes,Array[Byte]]] = Materialized.as(this.storeName)
    refererHitsCount.aggregate(() => "0", (key: String, aggOne: String, aggTwo: String) => {
        val value: Long = aggOne.toLong + aggTwo.toLong
        value.toString
    },store)
    val topology: Topology = builder.build
    return new KafkaStreams(topology, this.props)
  }

  private def runPurchaseCountStat: KafkaStreams = {
    val builder: StreamsBuilder = new StreamsBuilder
    val sourceHits: KStream[String, String] = builder.stream(topic)
    val refererCountOfPurchases: KGroupedStream[String, String] = sourceHits.mapValues((value: String) => new WebRecord(value)
      .getPurchasesCount(this.KEY_FIELD)).map((key: String, keyValue: KeyValue[String,String]) => keyValue)
      .groupBy((key: String, value: String) => key, Serialized.`with`(Serdes.String, Serdes.String))

    this.storeName = "sales"
    val store: Materialized[String,String, KeyValueStore[Bytes,Array[Byte]]] = Materialized.as(this.storeName)
    refererCountOfPurchases.aggregate(() => "0", (key: String, aggOne: String, aggTwo: String) => {
      val value: Long = aggOne.toLong + aggTwo.toLong
      value.toString
    },store)
    val topology: Topology = builder.build
    return new KafkaStreams(topology, this.props)
  }

  private def runCountStat: KafkaStreams = {
    val builder: StreamsBuilder = new StreamsBuilder
    val sourceHits: KStream[String, String] = builder.stream(topic)
    val refererValueOfPurchases: KGroupedStream[String, String] = sourceHits.mapValues((value: String) => new WebRecord(value)
      .getCountPair(this.KEY_FIELD)).map((key: String, keyValue: KeyValue[String,String]) => keyValue)
      .groupBy((key: String, value: String) => key, Serialized.`with`(Serdes.String, Serdes.String))

    this.storeName = "hits"
    val store: Materialized[String,String, KeyValueStore[Bytes,Array[Byte]]] = Materialized.as(this.storeName)
    refererValueOfPurchases.aggregate(() => "0", (key: String, aggOne: String, aggTwo: String) => {
      val value: Long = aggOne.toLong + aggTwo.toLong
      value.toString
    },store)
    val topology: Topology = builder.build
    return new KafkaStreams(topology, this.props)
  }
}

