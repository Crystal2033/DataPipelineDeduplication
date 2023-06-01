package ru.mai.lessons.rpks.processors.impl;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONException;
import org.json.JSONObject;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;
import ru.mai.lessons.rpks.processors.interfaces.RuleProcessor;
import ru.mai.lessons.rpks.redis.interfaces.RedisClient;

import java.util.List;
import java.util.Optional;

@Slf4j
@AllArgsConstructor
public class DeduplicationProcessor implements RuleProcessor {
    private final RedisClient redis;

    @Override
    public Optional<Message> processing(Message message, List<Rule> rules) {
        try {
            sendIfNotDuplicateAndSetMessageState(message, rules);
        } catch (JSONException ex) {
            log.error("Parsing json message {} error: ", message.getValue(), ex);
            return Optional.empty();
        }
        return Optional.of(message);
    }

    private void appendNewValueInKey(StringBuilder keyBuilder, String newValue) {
        if (!keyBuilder.isEmpty()) {
            keyBuilder.append(":");
        }
        keyBuilder.append(newValue);
    }

    private void sendIfNotDuplicateAndSetMessageState(Message checkingMessage, List<Rule> rules) throws JSONException {
        JSONObject jsonMessage = new JSONObject(checkingMessage.getValue());
        StringBuilder redisKeyBuilder = new StringBuilder();
        long expireTimeSec = 0;
        for (Rule rule : rules) {
            if (jsonMessage.keySet().contains(rule.getFieldName())) {
                appendNewValueInKey(redisKeyBuilder, jsonMessage.get(rule.getFieldName()).toString());
                if (expireTimeSec < rule.getTimeToLiveSec()) {
                    expireTimeSec = rule.getTimeToLiveSec();
                }
            }
        }
        redis.sendExpiredMessageIfNotExists(checkingMessage, redisKeyBuilder.toString(), expireTimeSec);
    }
}
