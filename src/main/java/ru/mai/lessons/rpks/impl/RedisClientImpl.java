package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.params.SetParams;
import ru.mai.lessons.rpks.RedisClient;


@Slf4j
public final class RedisClientImpl implements RedisClient, AutoCloseable {
    private final JedisPooled jedisPooled;


    public RedisClientImpl(Config config) {
        jedisPooled = new JedisPooled(config.getString("host"), config.getInt("port"));

        log.info("Redis client created");
    }

    @Override
    public void close() {
        try {
            jedisPooled.close();
        } catch (Exception ex) {
            log.info(ex.getMessage());
        }
    }

    @Override
    public boolean hasKey(String key) {
        return jedisPooled.exists(key);
    }

    @Override
    public void addKey(String key, Long secondsToExpire) {
        if (secondsToExpire != null) {
            jedisPooled.set(key, "", new SetParams().ex(secondsToExpire));
        } else {
            jedisPooled.set(key, "");
        }
    }
}
