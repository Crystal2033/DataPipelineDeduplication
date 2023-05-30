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
    @NonNull
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

//    public void writeData(String key, String value) {
//        log.info("Save data to Redis: {} = {}", key, value);
//        getJedis().set(key, value);
//        if (enabledAudit) {
//            getJedis().publish(AUDIT_CHANNEL, "set key " + key + ", value " + value);
//        }
//    }
//
//    public String readData(String key) {
//        log.info("Read data from Redis by key: {}", key);
//        if (enabledAudit) {
//            getJedis().publish(AUDIT_CHANNEL, "get " + key);
//        }
//        return getJedis().get(key);
//    }
//
//    public void expire(String key, long seconds) {
//        log.info("Set time to live {} seconds by key {}", seconds, key);
//        getJedis().expire(key, seconds);
//        if (enabledAudit) {
//            getJedis().publish(AUDIT_CHANNEL, "expire " + key + " " + seconds + " seconds");
//        }
//    }
//
//    public void audit() {
//        log.info("Enable audit. Create pub/sub channel '{}'", AUDIT_CHANNEL);
//        getJedis().subscribe(new Audit(), AUDIT_CHANNEL);
//        enabledAudit = true;
//    }

    private JedisPooled getJedis() {
        return Optional.ofNullable(jedis).orElse(new JedisPooled(host, port));
    }
}
