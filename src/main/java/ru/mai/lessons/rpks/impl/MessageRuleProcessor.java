package ru.mai.lessons.rpks.impl;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.util.*;
@Slf4j
public class MessageRuleProcessor implements RuleProcessor {

    private final MyRedisClient redisClient;
    public MessageRuleProcessor(MyRedisClient redisClient) {
        this.redisClient = redisClient;

    }
    private final ObjectMapper parser = new ObjectMapper();
    @Override
    public Message processing(Message message, Rule[] rules) {
        String value = message.getValue();
        message.setDeduplicationState(true);
        if (rules.length == 0) {
            return message;
        }

        rules = Arrays.stream(rules).filter(Rule::getIsActive).toArray(Rule[]::new);

        var fieldNames = Arrays.stream(rules).map(Rule::getFieldName).sorted().toArray(String[]::new);
        if (fieldNames.length == 0) {
            return message;
        }

        var ttl = Arrays.stream(rules).mapToLong(Rule::getTimeToLiveSec).max().orElse(0L);

        try {
            JsonNode jsonNode = parser.readTree(value);
            Map<String, String> keyMap = new HashMap<>();
            for (var fieldName: fieldNames) {
                var node = jsonNode.path(fieldName);
                if (!node.isMissingNode()) {
                    keyMap.put(fieldName, node.asText());
                }
            }
            if (keyMap.isEmpty()) {
                message.setDeduplicationState(false);
                return message;
            }

            if (!redisClient.containsKey(keyMap)) {
                redisClient.write(keyMap, keyMap, ttl);
            }
            else {
                message.setDeduplicationState(false);
            }
            return message;
        }
        catch (JsonProcessingException  e) {
            message.setDeduplicationState(false);
        }
        return message;
    }
}
