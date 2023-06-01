package ru.mai.lessons.rpks.processors.impl;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONException;
import org.json.JSONObject;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;
import ru.mai.lessons.rpks.processors.interfaces.RuleProcessor;
import ru.mai.lessons.rpks.redis.interfaces.RedisClient;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Slf4j
@AllArgsConstructor
public class DeduplicationProcessor implements RuleProcessor {
    private final RedisClient redis;

    @Override
    public Message processing(Message message, Map<String, List<Rule>> rules) {
        try {
            sendIfNotDuplicateAndSetMessageState(message, rules);
        } catch (JSONException ex) {
            //log.error("Parsing json message {} error: ", message.getValue(), ex);
            return null;
        }
        return message;
    }

    private String getRedisKeyByFields(String checkingMessage, Map<String, List<Rule>> rulesMap) throws JSONException {
        StringBuilder keyBuilder = new StringBuilder();

        JSONObject jsonObject = new JSONObject(checkingMessage);
        for (String key : jsonObject.keySet()) {
            if (rulesMap.containsKey(key)) {
                appendNewValueInKey(keyBuilder, jsonObject.get(key).toString());
            }
        }
        return keyBuilder.toString();
    }


    private void appendNewValueInKey(StringBuilder keyBuilder, String newValue) {
        if (!keyBuilder.isEmpty()) {
            keyBuilder.append(":");
        }
        keyBuilder.append(newValue);
    }

    private void sendIfNotDuplicateAndSetMessageState(Message checkingMessage, Map<String, List<Rule>> rulesMap) throws JSONException {
        String keyByRuleFields = getRedisKeyByFields(checkingMessage.getValue(), rulesMap);
        long expireTimeInSec = getTotalExpireTimeByExistingRules(rulesMap);
        redis.sendExpiredMessageIfNotExists(checkingMessage, keyByRuleFields, expireTimeInSec);
    }

    private long getTotalExpireTimeByExistingRules(Map<String, List<Rule>> rulesMap) {
        long expireTimeInSec = 0;
        for (var listOfRules : rulesMap.values()) {
            expireTimeInSec = getMaxExpireTimeByField(listOfRules, expireTimeInSec);
        }
        return expireTimeInSec;
    }

    private long getMaxExpireTimeByField(List<Rule> rules, long currentExpireTime) {
        if (rules != null) {
            Optional<Rule> ruleWithMaxExpireTime = rules.stream()
                    .max(Comparator.comparing(Rule::getTimeToLiveSec));
            return Math.max(currentExpireTime,
                    ruleWithMaxExpireTime.isEmpty() ? 0 : ruleWithMaxExpireTime.get().getTimeToLiveSec());
        } else {
            return currentExpireTime;
        }
    }

}
