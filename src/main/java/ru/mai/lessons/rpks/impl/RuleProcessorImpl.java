package ru.mai.lessons.rpks.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jooq.tools.json.JSONObject;
import org.jooq.tools.json.JSONParser;
import org.jooq.tools.json.ParseException;
import ru.mai.lessons.rpks.RedisClient;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Slf4j
@RequiredArgsConstructor
public final class RuleProcessorImpl implements RuleProcessor {
    private final RedisClient redisClient;

    @Override
    public Message processing(Message message, Rule[] rules) {
        if (rules == null || rules.length == 0) {
            message.setDeduplicationState(true);
            return message;
        }

        List<String> activeFieldNames = getSortedActiveFieldNamesFromRules(rules);
        if (activeFieldNames.isEmpty()) {
            //log.info("No active filed");
            message.setDeduplicationState(true);
            return message;
        }

        List<String> activeFieldValues = getActiveFieldValuesFromMessage(message, activeFieldNames);
        if (activeFieldValues.isEmpty()) {
            //log.info("Message is empty!");
            message.setDeduplicationState(false);
            return message;
        }

        long secondsToLive = getMaxTimeToLiveSecFromActiveRules(rules);

        String combinedActiveFieldNames = combineStringListToString(activeFieldNames);
        String combinedActiveFieldValues = combineStringListToString(activeFieldValues);

        if (!redisClient.containsKey(combinedActiveFieldValues)) {
            //log.info("message {} is not in redis", message.getValue());
            message.setDeduplicationState(true);
            redisClient.writeData(combinedActiveFieldValues, combinedActiveFieldNames, secondsToLive);
            return message;
        }

        String data = redisClient.getData(combinedActiveFieldValues);
        if (data.equals(combinedActiveFieldNames)) {
            //log.info("message {} already in redis", message.getValue());
            message.setDeduplicationState(false);
        } else {
            log.info("update value of message {} in redis", message.getValue());
            redisClient.writeData(combinedActiveFieldValues, combinedActiveFieldNames, secondsToLive);
            message.setDeduplicationState(true);
        }
        return message;
    }

    private long getMaxTimeToLiveSecFromActiveRules(Rule[] rules) {
        return Arrays.stream(rules)
                .filter(Rule::getIsActive)
                .mapToLong(Rule::getTimeToLiveSec)
                .max()
                .orElse(0);
    }

    private List<String> getSortedActiveFieldNamesFromRules(Rule[] rules) {
        return Arrays.stream(rules)
                .filter(Rule::getIsActive)
                .map(Rule::getFieldName)
                .sorted()
                .toList();
    }

    private List<String> getActiveFieldValuesFromMessage(Message message, List<String> activeFieldNames) {
        try {
            Object object = new JSONParser().parse(message.getValue());
            JSONObject jsonObject = (JSONObject) object;

            return activeFieldNames.stream()
                    .map(fieldName -> jsonObject.get(fieldName).toString())
                    .toList();
        } catch (ParseException e) {
            return new ArrayList<>();
        }
    }

    private String combineStringListToString(List<String> strings) {
        StringBuilder stringBuilder = new StringBuilder();
        strings.forEach(string -> {
            if (stringBuilder.length() != 0) {
                stringBuilder.append(':');
            }
            stringBuilder.append(string);
        });
        return stringBuilder.toString();
    }
}
