package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPooled;
import ru.mai.lessons.rpks.RedisClient;

import java.util.List;
import java.util.Set;

public class RedisClientRealization implements RedisClient {

    Jedis jedis;
    JedisPool jedisPool;
    public RedisClientRealization(Config config) {

        jedisPool = new JedisPool(config.getString("redis.host"), config.getInt("redis.port"));
        jedis = jedisPool.getResource();
    }
    public String getValue(String key) {
        return jedis.get(key);
    }

    public void insert(String key, Long time) {
        jedis.set(key, key);
        jedis.expire(key, time);
    }

    public Set<String> getKeys(String s) {
        return jedis.keys("*");
    }
}
