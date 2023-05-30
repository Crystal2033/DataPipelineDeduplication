package ru.mai.lessons.rpks.impl;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.JedisPooled;
import ru.mai.lessons.rpks.RedisClient;

import java.util.Optional;

@Slf4j
@RequiredArgsConstructor

public class RedisClientImpl implements RedisClient {
    @NonNull
    private final String host;
    private final int port;
    private JedisPooled jedis;

    public boolean existsKey(String value, long time) {
        if (getJedis().exists(value)) {
            return false;
        } else {
            getJedis().set(value, "");
            getJedis().expire(value, time);
            return true;
        }

    }
    private JedisPooled getJedis() {
        return Optional.ofNullable(jedis).orElse(new JedisPooled(host, port));
    }
}
