package bigdata.project;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;

import java.io.File;
import java.nio.file.Files;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/*
* All the methods and fields are private save for the constructor so that all statistic calls are controlled and
* fixed to one for each instance of the class
*/

//19,1499882547,Platform e.g Android 4.0.2,instagram.*,HTC Desire HD,0,5000
public class Aggregate{
    private final String topic;
    private final Properties props;
    private final Integer KEY_FIELD; // the statistic of count, purchase count and purchase value is made against me

    public Aggregate(Integer key, String brokers, String topic, Integer statistic, String rpcEndpoint, Integer
            rpcPort) throws Exception {
        //statistic
        // restricted to 0-2
        // count, purchase count and purchase value respectively
        // endpoint exposes the app info to the world
        this.topic = topic;
        this.KEY_FIELD = key;
        this.props  = new Properties();
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

        switch (statistic){
            case 2:
                streams = runPurchaseValueStat();
                break;
            case 1:
                streams = runPurchaseCountStat();
                break;
            default:
                streams = runCountStat();
        }
        streams.start();
        final RPCService restService =  startRestProxy(streams,rpcPort);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                streams.close();
                restService.stop();
            } catch (Exception e) {
                // ignored
            }
        }));
        /*final CountDownLatch latch = new CountDownLatch(1);
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
                    //
                }
                latch.countDown();
            }
        });

        System.exit(0);*/

    }
    private KafkaStreams runPurchaseValueStat(){
        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> sourceHits = builder.stream(this.topic);
        KGroupedStream<String, String> refererHitsCount = sourceHits
                .mapValues(value -> new WebRecord(value).getPurchasesCount(this.KEY_FIELD))
                .map((key,keyValue)->keyValue)
                .groupBy((key, value) -> key,
                        Serialized.with(
                                Serdes.String(),
                                Serdes.String()));

        final Materialized store = Materialized.as("value-stats");
        refererHitsCount.aggregate(
                    ()-> "0",
                    (key, aggOne,aggTwo) -> {
                        Long val = Long.parseLong(aggOne) + Long.parseLong(aggTwo);
                        return  val.toString();
                    },store);
        final Topology topology = builder.build();
        return new KafkaStreams(topology, this.props);
    }
    private KafkaStreams runPurchaseCountStat(){
        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> sourceHits = builder.stream(topic);
        KGroupedStream<String, String> refererCountOfPurchases = sourceHits
                .mapValues(value -> new WebRecord(value).getPurchasesCount(this.KEY_FIELD))
                .map((key,keyValue)->keyValue)
                .groupBy((key, value) -> key,
                        Serialized.with(
                                Serdes.String(),
                                Serdes.String()));
        final Materialized store = Materialized.as("sales-stats");
        refererCountOfPurchases.aggregate(
                    ()-> "0",
                    (key, aggOne,aggTwo) -> {
                        Long val = Long.parseLong(aggOne) + Long.parseLong(aggTwo);
                        return  val.toString();
                    },store);
        final Topology topology = builder.build();
        return new KafkaStreams(topology, this.props);
    }
    private KafkaStreams runCountStat(){
        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> sourceHits = builder.stream(topic);
        KGroupedStream<String, String> refererValueOfPurchases = sourceHits
                .mapValues(value -> new WebRecord(value).getCountPair(this.KEY_FIELD))
                .map((key,keyValue)->keyValue)
                .groupBy((key, value) -> key,
                Serialized.with(
                        Serdes.String(),
                        Serdes.String()));
        final Materialized store = Materialized.as("hits-stats");
        refererValueOfPurchases
                .aggregate(
                    ()-> "0",
                    (key, aggOne,aggTwo) -> {
                        Long val = Long.parseLong(aggOne) + Long.parseLong(aggTwo);
                        return  val.toString();

                    },store);
                //reduce((a,b) -> a+b,Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as
                //("word-count")).print();//,Materialized.as("hits-stats"));
        final Topology topology = builder.build();
        return new KafkaStreams(topology, this.props);
    }
    static RPCService startRestProxy(final KafkaStreams streams, final int port)
            throws Exception {
        final RPCService
                aggregateRestService = new RPCService(streams);
        aggregateRestService.start(port);
        return aggregateRestService;
    }
}
