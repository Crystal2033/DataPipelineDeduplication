package ru.mai.lessons.rpks.redis.impl;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;
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

    private boolean enabledAudit = false;

    private static final String AUDIT_CHANNEL = "audit";

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

    @Override
    public Message getMessageByStringAndTryToInsertInRedis(String stringMessage, String key, long expireTimeInSec) {
        if(getJedis().exists(key)){
            return Message.builder()
                    .value(stringMessage)
                    .isDuplicate(true)
                    .build();
        }
        else{
            getJedis().setex(key, expireTimeInSec ,stringMessage);
            return Message.builder()
                    .value(stringMessage)
                    .isDuplicate(false)
                    .build();
        }
    }
}
