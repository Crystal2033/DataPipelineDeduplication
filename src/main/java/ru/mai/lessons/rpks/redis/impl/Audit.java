package ru.mai.lessons.rpks.redis.impl;

import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.JedisPubSub;

@Slf4j
public class Audit extends JedisPubSub {

    @Override
    public void onSubscribe(String channel, int subscribedChannels) {
        log.info("Subscribe to channel name: {}", channel);
    }

    @Override
    public void onUnsubscribe(String channel, int subscribedChannels) {
        log.info("Unsubscribe to channel name: {}", channel);
    }

    @Override
    public void onMessage(String channel, String message) {
        log.info("Get message {} from channel {}", message, channel);
        if (message.contains("exit")) {
            unsubscribe();
        }
    }
}
