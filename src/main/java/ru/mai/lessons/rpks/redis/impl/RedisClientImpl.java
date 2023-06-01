package ru.mai.lessons.rpks.redis.impl;

import com.typesafe.config.Config;
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

    private final Config conf;

    private JedisPooled jedis;


    private JedisPooled getJedis() {
        return Optional.ofNullable(jedis).orElse(
                new JedisPooled(conf.getConfig("redis").getString("host"),
                        conf.getConfig("redis").getInt("port")));
    }

    public synchronized void sendExpiredMessageIfNotExists(Message message, String key, long expireTimeInSec) {
        if (getJedis().exists(key)) {
            log.info("Key {} exists in redis! Message {}", key, message.getValue());
            message.setDuplicate(true);

        } else {
            log.info("Set time to live {} seconds by key {} and message {}", expireTimeInSec, key, message.getValue());
            getJedis().set(key, "");
            getJedis().expire(key, expireTimeInSec);
            message.setDuplicate(false);
        }
    }
}
