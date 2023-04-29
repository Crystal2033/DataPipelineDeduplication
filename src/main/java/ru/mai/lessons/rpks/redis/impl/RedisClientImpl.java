package ru.mai.lessons.rpks.redis.impl;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.JedisPooled;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.redis.interfaces.RedisClient;

import java.util.Optional;

@Slf4j
@Data
@RequiredArgsConstructor
public class RedisClientImpl implements RedisClient {

    private final String host;
    private final int port;

    private JedisPooled jedis;

    private JedisPooled getJedis() {
        return Optional.ofNullable(jedis).orElse(new JedisPooled(host, port));
    }

    @Override
    public synchronized Message getMessageByStringAndTryToInsertInRedis(String stringMessage, String key, long expireTimeInSec) {
        if (getJedis().exists(key)) {
            log.info("Key {} exists in redis! Message {}", key, stringMessage);
            return Message.builder()
                    .value(stringMessage)
                    .isDuplicate(true)
                    .build();
        } else {
            log.info("Set time to live {} seconds by key {} and message {}", expireTimeInSec, key, stringMessage);
            getJedis().setex(key, expireTimeInSec, stringMessage);
            return Message.builder()
                    .value(stringMessage)
                    .isDuplicate(false)
                    .build();
        }
    }
}
