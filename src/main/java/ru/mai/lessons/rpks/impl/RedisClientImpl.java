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

    public boolean isExist(String key, long timeToLive) {
        if (getJedis().exists(key)) {
            log.debug("Rules [{}] with time live {} is exist", key, timeToLive);
            return false;
        }
        getJedis().set(key, "");
        log.debug("Rules [{}] with time live {} add", key, timeToLive);
        getJedis().expire(key, timeToLive);
        return true;
    }

    private JedisPooled getJedis() {
        return Optional.ofNullable(jedis).orElse(new JedisPooled(host, port));
    }


}
