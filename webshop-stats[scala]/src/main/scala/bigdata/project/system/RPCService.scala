package bigdata.project.system

import javax.ws.rs.core.MediaType
import javax.ws.rs.{GET, Path, PathParam, Produces}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.state.{KeyValueIterator, QueryableStoreTypes}
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}
import org.glassfish.jersey.jackson.JacksonFeature
import org.glassfish.jersey.server.ResourceConfig
import org.glassfish.jersey.servlet.ServletContainer


/**
  * Rest endpoint to broadcast the computed statistics in the application
  * available endpoints :
  * url:port/stats/{storeName}/all
  * url:port/stats/{storeName}/{key}
  * url:port/instances
  * url:port/instances/{storeName}
  * url:port/instance/{storeName}/{key}
  *
  */
@Path("/") class RPCService private[project](val streams: KafkaStreams) {
  private val metadataService = new MetadataService(streams)
  private var jettyServer = new Server()


  private def buildJson(streamData: KeyValueIterator[String, String]) = {
    var json = "["
    var i = 0
    while ( {
      streamData.hasNext
    }) {
      val record = streamData.next
      if (i ne 0) json += ","
      json += "{"
      json += "\"key\":\"" + record.key + "\",\"value\":" + "\"" + record.value + "\""
      json += "}"
      i += 1
    }
    json += "]"
    json
  }

  /**
    * Get all of the key-value pairs available in a store
    *
    * @param storeName store to query
    * @return A List representing all of the key-values in the provided
    *         store
    */
  @GET
  @Path("stats/{storeName}/all")
  @Produces(Array(MediaType.APPLICATION_JSON)) def allForStore(@PathParam("storeName") storeName: String): String = {
    streams.store(storeName, QueryableStoreTypes.keyValueStore[String, String]).all
    "hello mars"
  }

  @GET
  @Path("/instances")
  @Produces(Array(MediaType.APPLICATION_JSON)) def streamsMetadata: String = metadataService.streamsMetadata.toString

  /**
    * Get the metadata for all instances of this Kafka Streams application that currently
    * has the provided store.
    *
    * @param store The store to locate
    * @return List of { @link HostStoreInfo}
    */
  @GET
  @Path("/instances/{storeName}")
  @Produces(Array(MediaType.APPLICATION_JSON)) def streamsMetadataForStore(@PathParam("storeName") store: String): String = metadataService.streamsMetadataForStore(store)

  /**
    * Find the metadata for the instance of this Kafka Streams Application that has the given
    * store and would have the given key if it exists.
    *
    * @param store Store to find
    * @param key   The key to find
    * @return { @link HostStoreInfo}
    */
  @GET
  @Path("/instance/{storeName}/{key}")
  @Produces(Array(MediaType.APPLICATION_JSON)) def streamsMetadataForStoreAndKey(@PathParam("storeName") store: String, @PathParam("key") key: String): String = metadataService.streamsMetadataForStoreAndKey(store, key, new StringSerializer)

  /**
    * Start an embedded Jetty Server on the given port
    *
    * @param port port to run the Server on
    * @throws Exception
    */
  @throws[Exception]
  private[project] def start(port: Int): Unit = {
    val context = new ServletContextHandler(ServletContextHandler.SESSIONS)
    context.setContextPath("/")
    jettyServer = new Server(port)
    jettyServer.setHandler(context)

    val rc = new ResourceConfig
    rc.register(this)
    rc.register(classOf[JacksonFeature])

    val sc = new ServletContainer(rc)
    val holder = new ServletHolder(sc)
    context.addServlet(holder, "/*")

    jettyServer.start
  }

  /**
    * Stop the Jetty Server
    *
    * @throws Exception
    */
  @throws[Exception]
  private[project] def stop(): Unit = {
    if (jettyServer != null) jettyServer.stop
  }
}
