package bigdata.project;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/*
 * just pipe data through the stream using
 */
public class Pipe {
    public static void main(String[] args) throws Exception{
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-web-data");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);

        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream("clients-purchases").to("clients-purchases-output");

        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology,props);
        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook"){
            @Override
            public void run(){
                streams.close();
                latch.countDown();
            }
        });
        try{
            streams.start();
            latch.await();
        }catch (Throwable e){
            System.exit(1);
        }
        System.exit(0);

    }
}
