package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.JedisPooled;
import ru.mai.lessons.rpks.RedisClient;
import java.util.Optional;


@Slf4j
public class RedisClientImpl implements RedisClient {
    private final JedisPooled jedis;

    public RedisClientImpl(Config config) {
        jedis = new JedisPooled(config.getString("host"), config.getInt("port"));
    }

    @Override
    public String read(String key) {
        return jedis.get(key);
    }

    @Override
    public void write(String key, String value, long time) {
        jedis.set(key, value);
        jedis.expire(key, time);
    }

    @Override
    public boolean containsKey(String key) {
        return Optional.ofNullable(jedis.get(key)).isPresent();
    }
}