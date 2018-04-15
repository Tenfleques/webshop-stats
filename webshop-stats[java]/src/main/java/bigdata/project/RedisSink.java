package bigdata.project;

import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;
import com.lambdaworks.redis.RedisURI;
import org.apache.kafka.streams.KeyValue;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;


public class RedisSink {
    RedisClient redisClient;
    RedisSink(String redishost, int redisport){
        try {
            this.redisClient = new RedisClient(
                    RedisURI.create("redis://"+redishost+":"+redisport));
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    private String makeDateKey(Long milliseconds){
        return new java.sql.Date(milliseconds).toString().replace('-','_');
    }
    public void sinkDatedRecord(Long milliseconds, String record){
        RedisConnection<String, String> connection = redisClient.connect();
        connection.sadd(makeDateKey(milliseconds),record);
        connection.close();
    }
    private KeyValue<String,String> getAggregatePair(String record, String storeName){
        //example storeName = date-sales
        //st[1] = hits | sales | revenue
        String[] st = storeName.split("-");
        HashMap<String,Integer> stats = new HashMap<>();
        stats.put("date",RecordFields.DATE_FIELD.getValue());
        stats.put("platform",RecordFields.PLATFORM_FIELD.getValue());
        stats.put("referer",RecordFields.REFERER_FIELD.getValue());
        stats.put("item",RecordFields.ITEM_FIELD.getValue());
        stats.put("price",RecordFields.PRICE_FIELD.getValue());

        switch(st[1]){
            case "hits":
                return new WebRecord(record).getCountPair(stats.get(st[0]));
            case "sales":
                return new WebRecord(record).getPurchasesCount(stats.get(st[0]));
            default:
                return new WebRecord(record).getPurchasesValue(stats.get(st[0]));
        }

    }
    public String getForDate(String storeName, Long milliseconds){
        RedisConnection<String, String> connection = redisClient.connect();
        Iterator<Map.Entry<String,Long>> aggregatedForDate =
                connection.smembers(makeDateKey(milliseconds))
                .stream()
                .map(record-> getAggregatePair(record,storeName))
                .collect(
                    Collectors.groupingBy(kv->kv.key,
                            Collectors.mapping(
                                rec -> Long.parseLong(rec.value),
                                    Collectors.summingLong(r -> r.longValue())
                            )
                    )
                ).entrySet().iterator();

        connection.close();
        return new StreamJSON(aggregatedForDate).getJson();
    }
    public String getForDatesRange(String storeName, Long startMilliseconds, Long endMilliseconds){
        String unionKeys = "";
        while(startMilliseconds <= endMilliseconds){
            unionKeys += makeDateKey(startMilliseconds) + " ";
            startMilliseconds += 1000*3600*24;
        }
        System.out.println(unionKeys);
        RedisConnection<String, String> connection = redisClient.connect();
        Iterator<Map.Entry<String,Long>> aggregatedForDate =
                connection.sunion(unionKeys)
                        .stream()
                        .map(record-> getAggregatePair(record,storeName))
                        .collect(
                                Collectors.groupingBy(kv->kv.key,
                                        Collectors.mapping(
                                                rec -> Long.parseLong(rec.value),
                                                Collectors.summingLong(r -> r.longValue())
                                        )
                                )
                        )
                        .entrySet()
                        .iterator();
        connection.sunion(unionKeys)
                .stream()
                .map(record-> getAggregatePair(record,storeName))
                .collect(
                        Collectors.groupingBy(kv->kv.key,
                                Collectors.mapping(
                                        rec -> Long.parseLong(rec.value),
                                        Collectors.summingLong(r -> r.longValue())
                                )
                        )
                ).forEach((k,v) -> System.out.println(k + v));
        connection.close();
        return new StreamJSON(aggregatedForDate).getJson();
    }
    public void close(){
        redisClient.shutdown();
    }
}
