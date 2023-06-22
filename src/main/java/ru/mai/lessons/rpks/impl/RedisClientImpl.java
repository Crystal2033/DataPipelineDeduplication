package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import ru.mai.lessons.rpks.RedisClient;

public class RedisClientImpl implements RedisClient {
    private JedisPool jedisPool;

    public RedisClientImpl(Config config) {
        String host = config.getString("redis.host");
        int port = config.getInt("redis.port");
        jedisPool = new JedisPool(host, port);
    }

    @Override
    public boolean findKey(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            return !jedis.keys(key).isEmpty();
        }
    }

    @Override
    public void insert(String key, String value, long timeout) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.set(key, value);
            jedis.expire(key, timeout);
        }
    }
}
