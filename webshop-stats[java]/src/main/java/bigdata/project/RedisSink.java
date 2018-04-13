package bigdata.project;

import com.lambdaworks.redis.*;

public class RedisSink {
    RedisSink(String redishost, int redisport){
        RedisClient redisClient = new RedisClient(
                RedisURI.create("redis://"+redishost+":"+redisport));
        RedisConnection<String, String> connection = redisClient.connect();

        System.out.println("Connected to Redis");

        connection.close();
        redisClient.shutdown();
    }
}
