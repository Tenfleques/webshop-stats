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
 *      url:port/instances
 *
 */
@Path("/")
public class RPCService {
    private final KafkaStreams streams;
    private final MetadataService metadataService;
    private Server jettyServer;
    private final RedisSink redisSink;
    RPCService(final KafkaStreams streams, final RedisSink redisSink) {
        this.streams = streams;
        this.metadataService = new MetadataService(streams);
        this.redisSink = redisSink;
    }
    /**
     * Get all of the key-value pairs for date
     * @param  storeName
     * @param date  to query
     * @return A List representing all of the key-values for the date given
     */
    @GET()
    @Path("/archive/{storeName}/{date}")
    @Produces(MediaType.TEXT_PLAIN)
    public String getForDate(@PathParam("storeName") final String storeName,
                                        @PathParam("date") final String date) {
        return redisSink.getForDate(storeName,Long.parseLong(date));
    }
    /**
     * Get all of the key-value pairs for date
     * @param dateStart
     * @param dateEnd
     * @return A List representing all of the key-values for the date given
     */
    @GET()
    @Path("/archive/{storeName}/{dateStart}/{dateEnd}")
    @Produces(MediaType.APPLICATION_JSON)
    public String allForDateRange(@PathParam("storeName") final String storeName,
                                  @PathParam("dateStart") final String dateStart,
                                  @PathParam("dateEnd") final String dateEnd) {
        return redisSink.getForDatesRange(storeName,Long.parseLong(dateStart),Long.parseLong(dateEnd));
    }
    /**
     * Get all of the key-value pairs available in a store
     * @param storeName   store to query
     * @return A List representing all of the key-values in the provided
     * store
     */
    @GET()
    @Path("/stats/{storeName}/all")
    @Produces(MediaType.APPLICATION_JSON)
    public String allForStore(@PathParam("storeName") final String storeName) {
        return new StreamJSON(streams.store(storeName, QueryableStoreTypes.<String, String>keyValueStore()).all()).getJson();
    }
    @GET()
    @Path("/instances")
    @Produces(MediaType.APPLICATION_JSON)
    public String streamsMetadata() {
        return metadataService.streamsMetadata().toString();
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
