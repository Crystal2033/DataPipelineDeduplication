package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import ru.mai.lessons.rpks.RedisClient;

public class RedisClientI implements RedisClient {
    private final JedisPool pool;
    public RedisClientI(Config conf){
        pool = new JedisPool(conf.getString("host"), conf.getInt("port"));
    }
    @Override
    public void writeRule(String key, long time) {
        try (Jedis jedis = pool.getResource()) {
            jedis.set(key, "");
            jedis.expire(key, time);
        }
    }

    @Override
    public boolean contiansRule(String key){
        try(Jedis jedis = pool.getResource()){
            return jedis.exists(key);
        }
    }
}
