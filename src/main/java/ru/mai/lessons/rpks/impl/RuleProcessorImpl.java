package ru.mai.lessons.rpks.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.RedisClient;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

@Slf4j
public class RuleProcessorImpl implements RuleProcessor {
    private final ObjectMapper mapper = new ObjectMapper();
    private final RedisClient redisClient;

    public RuleProcessorImpl(RedisClient redisClient) {
        this.redisClient = redisClient;
    }

    @Override
    public Message processing(Message message, Rule[] rules) {
        try {
            SortedMap<String, String> ruleList = new TreeMap<>();
            long maxKey = 0L;
            JsonNode jsonNode = mapper.readTree(message.getValue());

            for (Rule rule : rules) {
                if (Boolean.FALSE.equals(rule.getIsActive())) {
                    continue;
                }

                JsonNode temp = jsonNode.get(rule.getFieldName());
                if (temp == null || !temp.isValueNode()) {
                    message.setDeduplicationState(false);
                    return message;
                }

                String value = temp.asText();
                if (value == null || value.isEmpty()) {
                    message.setDeduplicationState(false);
                    return message;
                }

                if (rule.getTimeToLiveSec() > maxKey) {
                    maxKey = rule.getTimeToLiveSec();
                }

                ruleList.put(rule.getFieldName(), value);
            }

            String joinedList = ruleList.entrySet()
                    .stream()
                    .map(entry -> entry.getKey() + entry.getValue())
                    .collect(Collectors.joining("*", "{", "}"));

            if (redisClient.findKey(joinedList)) {
                log.debug("Duplicate message detected: rule: {}", joinedList);
                message.setDeduplicationState(false);
                return message;
            }

            redisClient.insert(joinedList, joinedList, maxKey);
            message.setDeduplicationState(true);
            return message;
        } catch (Exception e) {
            log.error("JSON error: {}", e.toString());
        }

        return message;
    }
}
