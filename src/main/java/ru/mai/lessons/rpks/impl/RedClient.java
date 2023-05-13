package ru.mai.lessons.rpks.impl;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.JedisPooled;

import java.util.Optional;

@Slf4j
@Data
@RequiredArgsConstructor
public class RedClient {
    private final String host;
    private final int port;
    private JedisPooled jedis;

    public void writeData(String key, String value) {
        getJedis().set(key, value);
    }

    public boolean existData(String key) {
        return getJedis().exists(key);
    }

    public void expire(String key, long seconds) {
        getJedis().expire(key, seconds);
    }

    private JedisPooled getJedis() {
        return Optional.ofNullable(jedis).orElse(new JedisPooled(host, port));
    }
}
