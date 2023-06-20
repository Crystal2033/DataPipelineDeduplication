package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import ru.mai.lessons.rpks.RedisClient;

public class RedisClientImpl implements RedisClient {
    JedisPool pool;
    Jedis jedis;
    public RedisClientImpl(Config config){
        String host = config.getString("redis.host");
        int port = config.getInt("redis.port");
        pool = new JedisPool(host, port);
        jedis = pool.getResource();
    }
    @Override
    public boolean findKey(String key) {
        var value = jedis.keys(key);
        return  !value.isEmpty();
    }

    @Override
    public void insert(String key, String value, long timeout) {
        jedis.set(key, value);
        jedis.expire(key, timeout);
    }


}
