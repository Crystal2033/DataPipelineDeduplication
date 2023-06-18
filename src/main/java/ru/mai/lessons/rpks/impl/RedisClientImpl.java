package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import ru.mai.lessons.rpks.RedisClient;

@Slf4j
public class RedisClientImpl implements RedisClient {

    private final JedisPool pool;

    public RedisClientImpl(Config conf){
        this.pool = new JedisPool(conf.getString("redis.host"), conf.getInt("redis.port"));
    }

    public void set(String key, String value) {
        try(Jedis myJedis = pool.getResource()) {
            myJedis.set(key, value);
        }
    }

    public boolean exist(String key) {
        try(Jedis myJedis = pool.getResource()) {
            return myJedis.exists(key);
        }
    }

    public void expire(String key, long seconds) {
        try(Jedis myJedis = pool.getResource()) {
            myJedis.expire(key, seconds);
        }
    }
}
