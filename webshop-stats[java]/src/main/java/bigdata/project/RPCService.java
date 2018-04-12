package bigdata.project;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

/**
 *  Rest endpoint to broadcast the computed statistics in the application
 *  available endpoints :
 *      url:port/stats/{storeName}/all
 *      url:port/stats/{storeName}/{key}
 *      url:port/instances
 *      url:port/instances/{storeName}
 *      url:port/instance/{storeName}/{key}
 *
 */
@Path("/")
public class RPCService {

    private final KafkaStreams streams;
    private final MetadataService metadataService;
    private Server jettyServer;

    RPCService(final KafkaStreams st) {
        this.streams = st;
        this.metadataService = new MetadataService(st);
    }
    private String buildJson(KeyValueIterator<String,String> streamData){
        String json = "[";
        Integer i = 0;
        while(streamData.hasNext()){
            KeyValue<String, String> record = streamData.next();
            if(i!=0)
                json += ",";
            json += "{";
            json += "\"key\":\""+record.key + "\",\"value\":" + "\""+ record.value+"\"";
            json += "}";
            i++;
        }
        json += "]";
        return json;
    }

    /**
     * Get all of the key-value pairs available in a store
     * @param storeName   store to query
     * @return A List representing all of the key-values in the provided
     * store
     */
    @GET()
    @Path("stats/{storeName}/all")
    @Produces(MediaType.APPLICATION_JSON)
    public String allForStore(@PathParam("storeName") final String storeName) {
        streams.store(storeName, QueryableStoreTypes.<String, String>keyValueStore()).all();
        return "hello mars";
    }

    @GET()
    @Path("/instances")
    @Produces(MediaType.APPLICATION_JSON)
    public String streamsMetadata() {
        return metadataService.streamsMetadata().toString();
    }


    /**
     * Get the metadata for all instances of this Kafka Streams application that currently
     * has the provided store.
     * @param store   The store to locate
     * @return  List of {@link HostStoreInfo}
     */
    @GET()
    @Path("/instances/{storeName}")
    @Produces(MediaType.APPLICATION_JSON)
    public String streamsMetadataForStore(@PathParam("storeName") String store) {
        return metadataService.streamsMetadataForStore(store);
    }

    /**
     * Find the metadata for the instance of this Kafka Streams Application that has the given
     * store and would have the given key if it exists.
     * @param store   Store to find
     * @param key     The key to find
     * @return {@link HostStoreInfo}
     */
    @GET()
    @Path("/instance/{storeName}/{key}")
    @Produces(MediaType.APPLICATION_JSON)
    public String streamsMetadataForStoreAndKey(@PathParam("storeName") String store,
                                                       @PathParam("key") String key) {
        return metadataService.streamsMetadataForStoreAndKey(store, key, new StringSerializer());
    }

    /**
     * Start an embedded Jetty Server on the given port
     * @param port    port to run the Server on
     * @throws Exception
     */
    void start(final int port) throws Exception {
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");

        jettyServer = new Server(port);
        jettyServer.setHandler(context);

        ResourceConfig rc = new ResourceConfig();
        rc.register(this);
        rc.register(JacksonFeature.class);

        ServletContainer sc = new ServletContainer(rc);
        ServletHolder holder = new ServletHolder(sc);
        context.addServlet(holder, "/*");

        jettyServer.start();
    }

    /**
     * Stop the Jetty Server
     * @throws Exception
     */
    void stop() throws Exception {
        if (jettyServer != null) {
            jettyServer.stop();
        }
    }

}
