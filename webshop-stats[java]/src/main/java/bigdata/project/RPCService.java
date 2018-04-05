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
import java.util.List;

/**
 *  A simple REST proxy that runs embedded in the {@link bigdata.project.Aggregate}. This is used to
 *  demonstrate how a developer can use the Interactive Queries APIs exposed by Kafka Streams to
 *  locate and query the State Stores within a Kafka Streams Application.
 */
@Path("state")
public class RPCService {

    private final KafkaStreams streams;
    private final MetadataService metadataService;
    private Server jettyServer;

    RPCService(final KafkaStreams streams) {
        this.streams = streams;
        this.metadataService = new MetadataService(streams);
    }

    /**
     * Get all of the key-value pairs available in a store
     * @param storeName   store to query
     * @return A List representing all of the key-values in the provided
     * store
     */
    @GET()
    @Path("/keyvalues/{storeName}/all")
    @Produces(MediaType.APPLICATION_JSON)
    public String allForStore(@PathParam("storeName") final String storeName) {
        //List<KeyValueBean> allInStore = rangeForKeyValueStore(storeName, ReadOnlyKeyValueStore::all);
        String json = "[";
        Integer i = 0;
        KeyValueIterator<String,String> data = streams
                .store(storeName, QueryableStoreTypes.<String, String>keyValueStore())
                .all();

        while(data.hasNext()){
            KeyValue<String, String> record = data.next();
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
     * Get all of the key-value pairs that have keys within the range from...to
     * @param //storeName   store to query
     * @param //from        start of the range (inclusive)
     * @param //to          end of the range (inclusive)
     * @return A JsonArray representing all of the key-values in the provided
     * store that fall withing the given range.
     */
    /*@GET()
    @Path("/keyvalues/{storeName}/range/{from}/{to}")
    @Produces(MediaType.APPLICATION_JSON)
    public List<KeyValueBean> keyRangeForStore(@PathParam("storeName") final String storeName,
                                               @PathParam("from") final String from,
                                               @PathParam("to") final String to) {
        return rangeForKeyValueStore(storeName, store -> store.range(from, to));
    }*/
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
    public List<HostStoreInfo> streamsMetadataForStore(@PathParam("storeName") String store) {
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
    public HostStoreInfo streamsMetadataForStoreAndKey(@PathParam("storeName") String store,
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
