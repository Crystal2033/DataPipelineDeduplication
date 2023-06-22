package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.JedisPooled;
import ru.mai.lessons.rpks.RedisClient;

@Slf4j
public class RedisClientImpl implements RedisClient {
    JedisPooled jedis;

    RedisClientImpl(Config config){
        log.info("host, port: " + config);
        jedis = new JedisPooled(config.getString("host"), config.getInt("port"));
    }

    public void addRule(String key, String value, Long time) {
        jedis.set(key, value);
        jedis.expire(key, time);
    }

    public boolean existKey(String key){
        return jedis.exists(key);
    }

}
