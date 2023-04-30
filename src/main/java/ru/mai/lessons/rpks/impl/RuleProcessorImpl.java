package ru.mai.lessons.rpks.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.RedisClient;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.io.IOException;
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
            message.setDeduplicationState(true);
            return message;
        }

        List<String> activeFieldValues = getActiveFieldValuesFromMessage(message, activeFieldNames);
        if (activeFieldValues.isEmpty()) {
            message.setDeduplicationState(false);
            return message;
        }

        long secondsToLive = getMaxTimeToLiveSecFromActiveRules(rules);

        String combinedActiveFieldNames = combineStringListToString(activeFieldNames);
        String combinedActiveFieldValues = combineStringListToString(activeFieldValues);

        if (!redisClient.containsKey(combinedActiveFieldValues)) {
            message.setDeduplicationState(true);
            redisClient.writeData(combinedActiveFieldValues, combinedActiveFieldNames, secondsToLive);
            return message;
        }

        String data = redisClient.getData(combinedActiveFieldValues);
        if (data.equals(combinedActiveFieldNames)) {
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
            ObjectNode object = new ObjectMapper().readValue(message.getValue(), ObjectNode.class);
            return activeFieldNames.stream()
                    .map(fieldName -> object.get(fieldName).toString())
                    .toList();
        } catch (IOException e) {
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
