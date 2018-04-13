package bigdata.project;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;

import java.io.File;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.CountDownLatch;

/*
* All the methods and fields are private save for the constructor so that all statistic calls are controlled and
* fixed to one for each instance of the class
*/

//19,1499882547,Platform e.g Android 4.0.2,instagram.*,HTC Desire HD,0,5000
public class Aggregate{
    private final String topic;
    private final Properties props;
    private final Integer DATE_FIELD = 1, PLATFORM_FIELD = 2, REFERER_FIELD = 3,
            ITEM_FIELD = 4, QUANTITY_FIELD = 5, PRICE_FIELD = 6;

    final StreamsBuilder builder;

    public Aggregate(String brokers, String topic, String rpcEndpoint, Integer
            rpcPort) throws Exception {
        //statistic
        // restricted to 0-2
        // count, purchase count and purchase value respectively
        // endpoint exposes the app info to the world
        this.topic = topic;
        this.props  = new Properties();
        this.builder = new StreamsBuilder();



        final String APP_ID = "stream-aggregate-data" + new Date().hashCode();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, rpcEndpoint + ":" + rpcPort);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        final File storeFile = Files.createTempDirectory(new File("/tmp").toPath(), APP_ID).toFile();
        props.put(StreamsConfig.STATE_DIR_CONFIG, storeFile.getPath());

        final KafkaStreams streams;
        KStream<String, String> source = this.builder.stream(this.topic);
        //expose the available REST endpoints
        String info = "\n" +
                "*available endpoints :\n";

                //TODO create platform to query the state of the given list
                //"*\t     http://"+rpcEndpoint+":"+rpcPort+"/"+storeName+"/{listofkeys}\n" +

        HashMap<Integer, String> stats = new HashMap<>();
        stats.put(DATE_FIELD,"date-");
        stats.put(PLATFORM_FIELD,"platform-");
        stats.put(REFERER_FIELD,"referer-");
        stats.put(ITEM_FIELD,"item-");
        stats.put(QUANTITY_FIELD,"quantity-");
        stats.put(PRICE_FIELD,"price-");

        //for()
        Iterator<Map.Entry<Integer,String>> stat = stats.entrySet().iterator();
        while(stat.hasNext()){
            Map.Entry<Integer,String> pair = stat.next();
            for (String storeName : runStats(source,pair.getKey(),pair.getValue())){
                info += "*\t     http://"+rpcEndpoint+":"+rpcPort+"/stats/"+storeName+"/all\n";
            }
        }

        info +=  "*\t     http://"+rpcEndpoint+":"+rpcPort+"/instances\n";
        final Topology topology = builder.build();
        streams = new KafkaStreams(topology, this.props);


        System.out.print(info);

        streams.start();
        final RPCService restService =  startRestProxy(streams,rpcPort);
        final CountDownLatch latch = new CountDownLatch(1);
        try{
            latch.await();
        }catch (Throwable e){
            System.exit(1);
        }
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook"){
            @Override
            public void run(){
                streams.close();
                try {
                    restService.stop();
                }catch (Exception e){
                    System.out.print(e.getMessage());
                }
                latch.countDown();
            }
        });
        System.exit(0);

    }
    private String[] runStats(KStream<String, String> source, final int KEY_FIELD, final String storePrefix){
        String hits = storePrefix+"hits";
        String sales = storePrefix+"sales";
        String revenue = storePrefix+"values";

        String[] listOfStores = new String[] {hits,sales,revenue};

        final Materialized hitsStore = Materialized.as(hits);
        final Materialized salesStore = Materialized.as(sales);
        final Materialized valueStore = Materialized.as(revenue);

        KGroupedStream<String, String> refererValueOfPurchases = source
                .mapValues(value -> new WebRecord(value).getCountPair(KEY_FIELD))
                .map((key,keyValue)->keyValue)
                .groupBy((key, value) -> key,
                Serialized.with(
                        Serdes.String(),
                        Serdes.String()));


        refererValueOfPurchases
                .aggregate(
                    ()-> "0",
                    (key, aggOne,aggTwo) -> {
                        Long val = Long.parseLong(aggOne) + Long.parseLong(aggTwo);
                        return  val.toString();

                    },hitsStore);

        /****/
        KGroupedStream<String, String> refererCountOfPurchases = source
                .mapValues(value -> new WebRecord(value).getPurchasesCount(KEY_FIELD))
                .map((key,keyValue)->keyValue)
                .groupBy((key, value) -> key,
                        Serialized.with(
                                Serdes.String(),
                                Serdes.String()));


        refererCountOfPurchases.aggregate(
                ()-> "0",
                (key, aggOne,aggTwo) -> {
                    Long val = Long.parseLong(aggOne) + Long.parseLong(aggTwo);
                    return  val.toString();
                },salesStore);

        /***/

        /***/
        KGroupedStream<String, String> refererHitsCount = source
                .mapValues(value -> new WebRecord(value).getPurchasesValue(KEY_FIELD))
                .map((key,keyValue)->keyValue)
                .groupBy((key, value) -> key,
                        Serialized.with(
                                Serdes.String(),
                                Serdes.String()));


        refererHitsCount.aggregate(
                ()-> "0",
                (key, aggOne,aggTwo) -> {
                    Long val = Long.parseLong(aggOne) + Long.parseLong(aggTwo);
                    return  val.toString();
                },valueStore);
        /***/

        return listOfStores;
    }
    static RPCService startRestProxy(final KafkaStreams streams, final int port)
            throws Exception {
        final RPCService
                aggregateRestService = new RPCService(streams);
        aggregateRestService.start(port);
        return aggregateRestService;
    }
}
