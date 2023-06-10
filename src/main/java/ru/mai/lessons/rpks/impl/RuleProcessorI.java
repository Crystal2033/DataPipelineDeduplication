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
public class RuleProcessorI implements RuleProcessor {
    RedisClientI redisClientI;
    public RuleProcessorI(RedisClientI redisClient){
        this.redisClientI = redisClient;
    }
    @Override
    public Message processing(Message message, Rule[] rules) {
        message.setDeduplicationState(true);
        if (rules.length == 0){
            return message;
        }
        rules = Arrays.stream(rules).filter(Rule::getIsActive).toArray(Rule[]::new);

        var fieldNames = Arrays.stream(rules).map(Rule::getFieldName).sorted().toArray(String[]::new);
        if (fieldNames.length == 0) {
           return message;
        }
        ObjectMapper mapper = new ObjectMapper();
        SortedMap<String, String> messageComplete = new TreeMap<>();
        String value = message.getValue();
        try {
            JsonNode node = mapper.readTree(value);
            String key;
            String valForMap;
            long maxTime = 0;
            for (Rule rule: rules){
                if (Boolean.TRUE.equals(rule.getIsActive())){
                    key = rule.getFieldName();
                    JsonNode val = node.path(rule.getFieldName());
                    if (!val.isMissingNode()) {
                        valForMap = String.valueOf(node.path(rule.getFieldName()));
                        messageComplete.put(key, valForMap);
                    }
                    if (rule.getTimeToLiveSec() > maxTime)
                        maxTime = rule.getTimeToLiveSec();
                }
            }
            if (messageComplete.isEmpty()){
                message.setDeduplicationState(false);
                return message;
            }
            String keys = mapper.writer().writeValueAsString(messageComplete);
            if (redisClientI.contiansRule(keys)){
                message.setDeduplicationState(false);
            } else {
                redisClientI.writeRule(keys, keys, maxTime);
            }

            return message;
        } catch (JsonProcessingException ex){
            log.error(ex.getMessage());
        }

        return message;
    }
}
