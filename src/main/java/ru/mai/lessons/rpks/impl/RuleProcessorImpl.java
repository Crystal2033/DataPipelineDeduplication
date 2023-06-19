package ru.mai.lessons.rpks.impl;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.RedisClient;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.ExpiredKey;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.util.Objects;

@Slf4j
@RequiredArgsConstructor
public final class RuleProcessorImpl implements RuleProcessor {

    private final RedisClient redisClient;

    private ExpiredKey makeExpiredKey(Message message, Rule[] rules) {
        JsonObject jsonMessage = JsonParser.parseString(message.getValue()).getAsJsonObject();
        JsonObject messageKey = new JsonObject();
        Long minExpiration = null;
        for (Rule rule : rules) {
            if (Boolean.TRUE.equals(rule.getIsActive())) {
                String curRuleFieldName = rule.getFieldName();
                if (jsonMessage.has(curRuleFieldName)) {
                    messageKey.add(curRuleFieldName, jsonMessage.get(curRuleFieldName));
                    if (minExpiration == null || rule.getTimeToLiveSec() < minExpiration) {
                        minExpiration = rule.getTimeToLiveSec();
                    }
                }
            }
        }
        return new ExpiredKey(messageKey.toString(), minExpiration);
    }

    @Override
    public Message processing(Message message, Rule[] rules) {
        message.setDeduplicationState(true);
        ExpiredKey expiredKey = makeExpiredKey(message, rules);
        if (!Objects.equals(expiredKey.key(), "{}")) {
            if (redisClient.hasKey(expiredKey.key())) {
                log.info("Redis already has key %s".formatted(expiredKey.key()));
                message.setDeduplicationState(false);
            } else {
                log.info("Create new redis key %s".formatted(expiredKey.key()));
                redisClient.addKey(expiredKey.key(), expiredKey.expiration());
            }
        }
        return message;
    }
}
