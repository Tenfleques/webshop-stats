package com.flequesboard.java.apps;

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
class AggregateKafka {

    private RedisSink redisSink;
    final StreamsBuilder builder;
    final KafkaStreams streams;

    AggregateKafka(String brokers, String topic, String rpcEndpoint, Integer
            rpcPort, String redishost, int redisport) throws Exception {
        //statistic
        // restricted to 0-2
        // count, purchase count and purchase value respectively
        // endpoint exposes the app info to the world
        final Properties props;
        props  = new Properties();
        this.builder = new StreamsBuilder();
        this.redisSink = new RedisSink(redishost, redisport);


        final String APP_ID = "stream-aggregate-data";
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, rpcEndpoint + ":" + rpcPort);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        final File storeFile = Files.createTempDirectory(new File("/tmp").toPath(), APP_ID).toFile();
        props.put(StreamsConfig.STATE_DIR_CONFIG, storeFile.getPath());


        KStream<String, String> source = this.builder.stream(topic);
        //sink records to redis by date key
        sinkDates(source);
        //expose the available REST endpoints
        StringBuilder info = new StringBuilder("\n" +
                "*available endpoints :\n");
        Iterator<Map.Entry<Integer,String>> stat = new StorePairFields().getValues();

        List<String> historicalStores = new ArrayList<>();
        while(stat.hasNext()){
            Map.Entry<Integer,String> pair = stat.next();
            for (String storeName : runLiveStats(source,pair.getKey(),pair.getValue())){
                info.append("*\t     http://").append(rpcEndpoint).append(":").append(rpcPort).append("/live/").append(storeName).append("/all\n");
                info.append("*\t     http://").append(rpcEndpoint).append(":").append(rpcPort).append("/archive/").append(storeName).append("/yyyy-mm-dd \n");
                info.append("*\t     http://").append(rpcEndpoint).append(":").append(rpcPort).append("/archive/").append(storeName).append("/yyyy-mm-dd").append("/yyyy-mm-dd\n");
                historicalStores.add(storeName);
            }
        }
        info.append("*\t     http://").append(rpcEndpoint).append(":").append(rpcPort).append("/archive/stores\n");
        info.append("*\t     http://").append(rpcEndpoint).append(":").append(rpcPort).append("/instances\n");
        info.append("*\t     http://").append(rpcEndpoint).append(":").append(rpcPort).append("/live/dates\n");

        final Topology topology = builder.build();
        streams = new KafkaStreams(topology, props);


        System.out.print(info);

        streams.start();
        final RPCService restService =  startRestProxy(streams,redisSink,rpcPort);
        restService.setArchiveStores(historicalStores);
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
                redisSink.close();
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
    private String makeStats(KStream<String, String> source, final int KEY_FIELD,final String storePrefix, final
                           int WHICH_STAT){

        String[] storage = new String[]{storePrefix+"revenue",storePrefix+"sales",storePrefix+"hits"};

        KGroupedStream<String, String> refererValueOfPurchases = source
                .mapValues(value -> {
                    WebRecord webRecord = new WebRecord(value);
                    switch (WHICH_STAT){
                        case 2:
                            return webRecord.getCountPair(KEY_FIELD);
                        case 1:
                            return webRecord.getPurchasesCount(KEY_FIELD);
                        default:
                            return webRecord.getPurchasesValue(KEY_FIELD);
                    }

                })
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
                        },Materialized.as(storage[WHICH_STAT]));
        return storage[WHICH_STAT];
    }
    private String[] runLiveStats(KStream<String, String> source, final int KEY_FIELD, final String storePrefix){
        String hits = makeStats(source,KEY_FIELD,storePrefix,2);
        String sales = makeStats(source,KEY_FIELD,storePrefix,1);
        String revenue = makeStats(source,KEY_FIELD,storePrefix,0);

        return new String[] {hits,sales,revenue};
    }
    private void sinkDates(KStream<String, String> source){
        KGroupedStream<String, String> dateRecords = source
                .mapValues(value -> new WebRecord(value).getDatedRecord())
                .map((key,keyValue)->keyValue)
                .map((date,record)-> {
                    redisSink.sinkDatedRecord(date,record);
                    return KeyValue.pair(new java.sql.Date(date).toString(),"1");
                })
                .groupBy((date, one) -> date,
                        Serialized.with(
                                Serdes.String(),
                                Serdes.String()));
        dateRecords
                .aggregate(
                        ()-> "0",
                        (key, aggOne,aggTwo) -> {
                            Long val = Long.parseLong(aggOne) + Long.parseLong(aggTwo);
                            return  val.toString();
                        },Materialized.as(AdministrativeStores.LIVE_DATES.getValue()));

    }
    private static RPCService startRestProxy(final KafkaStreams streams, final RedisSink redisSink, final int port)
            throws Exception {
        final RPCService
                aggregateRestService = new RPCService(streams, redisSink);
        aggregateRestService.start(port);
        return aggregateRestService;
    }
}
