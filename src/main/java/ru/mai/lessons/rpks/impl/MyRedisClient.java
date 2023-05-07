package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import ru.mai.lessons.rpks.RedisClient;

public class MyRedisClient implements RedisClient {
    JedisPool jedisPool;

    public MyRedisClient(Config config) {
        String ip = config.getConfig("redis").getString("host");
        int port = config.getConfig("redis").getInt("port");
        jedisPool = new JedisPool(new JedisPoolConfig(), ip, port);

    }

    public boolean checkExist(String value, long time) {
        try(Jedis jedis = jedisPool.getResource()){
            if (jedis.exists(value)){
                return false;
            }else{
                jedis.set(value, "_" + value);
                jedis.expire(value, time);
                return true;
            }
        }
    }

}
