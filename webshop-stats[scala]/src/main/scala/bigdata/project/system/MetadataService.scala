package bigdata.project.system

import java.util
import java.util.Collection
import java.util.stream.Collectors

import javax.ws.rs.NotFoundException
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.state.StreamsMetadata


/**
  * Looks up StreamsMetadata from KafkaStreams and converts the results to JSON strings.
  */
class MetadataService(val streams: KafkaStreams) {
  /**
    * Get the metadata for all of the instances of this Kafka Streams application
    *
    * @return List of { @link HostStoreInfo}
    */
  def streamsMetadata: String = { // Get metadata for all of the instances of this Kafka Streams application
    val metadata = streams.allMetadata
    jsonMetadata(metadata)
  }

  /**
    * Get the metadata for all instances of this Kafka Streams application that currently
    * has the provided store.
    *
    * @param store The store to locate
    * @return List of { @link HostStoreInfo}
    */
  def streamsMetadataForStore(store: String): String = { // Get metadata for all of the instances of this Kafka Streams application hosting the store
    val metadata = streams.allMetadataForStore(store)
    jsonMetadata(metadata)
  }

  /**
    * Find the metadata for the instance of this Kafka Streams Application that has the given
    * store and would have the given key if it exists.
    *
    * @param store Store to find
    * @param key   The key to find
    * @return { @link HostStoreInfo}
    */
  def streamsMetadataForStoreAndKey(store: String, key: String, serializer: Serializer[String]): String = {
    val metadata = streams.metadataForKey(store, key, serializer)
    if (metadata == null) throw new NotFoundException
    new HostStoreInfo(metadata.host, metadata.port, metadata.stateStoreNames).toString
  }

  private def jsonMetadata(metadata: Collection[StreamsMetadata]) = {
    var json = "["
    val i = 0
    val hostInfo : util.Iterator[StreamsMetadata]  = metadata.stream.collect(Collectors[_>:StreamsMetadata])
    while (hostInfo.hasNext) {
      val metadata = hostInfo.next
      if (i ne 0) json += ","
      json += new HostStoreInfo(metadata.host, metadata.port, metadata.stateStoreNames).toString
    }
    json += "]"
    json
  }
}
