package ru.mai.lessons.rpks.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.typesafe.config.Config;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import ru.mai.lessons.rpks.RedisClient;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;


public class MyRedisClient  implements RedisClient {

    private final JedisPool jedisPool;
    private static final ObjectMapper mapper = new ObjectMapper();

    public MyRedisClient(Config config) {
        jedisPool = new JedisPool(config.getString("host"), config.getInt("port"));
    }

    @Override
    public boolean containsKey(Map<String, String> key) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.exists(fromMap2String(key));
        }
    }

    @Override
    public void write(Map<String, String> key, long timeToLiveSec) {
        try (Jedis jedis = jedisPool.getResource()) {
            var keyJSON = fromMap2String(key);
            jedis.set(keyJSON, "");
            jedis.expire(keyJSON, timeToLiveSec);
        }
    }

    private String fromMap2String(Map<String, String> array) {
        try {
            return mapper.writer().writeValueAsString(array);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

}
