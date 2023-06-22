package ru.mai.lessons.rpks.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.util.HashMap;
import java.util.Map;

@Data
@Slf4j
public class RuleProcessorImpl implements RuleProcessor {

    private RedisClientImpl redisClient;
    private ObjectMapper mapper = new ObjectMapper();

    RuleProcessorImpl(RedisClientImpl redisClient) {
        this.redisClient = redisClient;
    }

    @Override
    public Message processing(Message message, Rule[] rules) {
        message.setDeduplicationState(true);

        if (rules.length == 0) {
            return message;
        }

        HashMap<String, String> messagesForCheck = new HashMap<>();
        String fieldValue;

        boolean flag = false;

        Long time = 0L;
        for (Rule rule :
                rules) {
            if (Boolean.FALSE.equals(rule.getIsActive())) {
                continue;
            }
            flag = true;
            fieldValue = getFieldValue(message.getValue(), rule.getFieldName());

            if (fieldValue != null) {
                messagesForCheck.put(rule.getFieldName(), fieldValue);
            }
            if (rule.getTimeToLiveSec() > time)
                time = rule.getTimeToLiveSec();
        }
        if (messagesForCheck.isEmpty()) {
            if (flag)
                message.setDeduplicationState(false);
            return message;
        }
        String keys = null;
        try {
            keys = mapper.writer().writeValueAsString(messagesForCheck);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        log.info("keys: " + keys);

        if (redisClient.existKey(keys)) {
            message.setDeduplicationState(false);
        } else {
            redisClient.addRule(keys, "", time);
        }

        return message;
    }

    private String getFieldValue(String value, String fieldName) {
        mapper = new ObjectMapper();

        Map<Object, Object> map = null;
        try {
            map = mapper.readValue(value, Map.class);
        } catch (JsonProcessingException e) {
            return null;
        }
        return (map.get(fieldName) == null ? (null) : (map.get(fieldName).toString()));
    }
}
