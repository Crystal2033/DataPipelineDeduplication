package ru.mai.lessons.rpks.redis.impl;

import com.typesafe.config.Config;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.JedisPooled;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.redis.interfaces.RedisClient;

import java.util.Optional;
import java.util.concurrent.Semaphore;

@Slf4j
@Data
@RequiredArgsConstructor
public class RedisClientImpl implements RedisClient {

    private final Config conf;

    private JedisPooled jedis;

    private static Object syncObj = new Object();
    private static Semaphore semaphore = new Semaphore(1);

    private JedisPooled getJedis() {
        return Optional.ofNullable(jedis).orElse(
                new JedisPooled(conf.getConfig("redis").getString("host"),
                        conf.getConfig("redis").getInt("port")));
    }

    public void sendExpiredMessageIfNotExists(Message message, String key, long expireTimeInSec) {

        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            log.error("Semaphore acquire problem. Interrupted.");
            Thread.currentThread().interrupt();
        }
        if (getJedis().exists(key)) {
            log.debug("Key {} exists in redis! Message {}", key, message.getValue());
            message.setDuplicate(true);

        } else {
            log.debug("Set time to live {} seconds by key {} and message {}", expireTimeInSec, key, message.getValue());
            getJedis().set(key, "");
            getJedis().expire(key, expireTimeInSec);
            message.setDuplicate(false);
        }
        semaphore.release();
    }
}
