package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.JedisPooled;
import ru.mai.lessons.rpks.RedisClient;

import java.util.Optional;

@Slf4j
public class RedisClientImpl implements RedisClient {
    private final String host;
    private final int port;

    private JedisPooled jedis;

    public RedisClientImpl(Config config) {
        host = config.getString("host");
        port = config.getInt("port");
    }

    @Override
    public boolean containsKey(String key) {
        return getJedis().get(key) != null;
    }

    @Override
    public void writeData(String key, String value, long seconds) {
        getJedis().set(key, value);
        getJedis().expire(key, seconds);
    }

    @Override
    public String getData(String key) {
        return getJedis().get(key);
    }

    private JedisPooled getJedis() {
        return Optional.ofNullable(jedis).orElse(new JedisPooled(host, port));
    }
}
