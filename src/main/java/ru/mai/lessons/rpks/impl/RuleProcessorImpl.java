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
    ObjectMapper mapper = new ObjectMapper();
    RedisClient redisClient;

    RuleProcessorImpl(RedisClient redis){
        redisClient = redis;
    }


    @Override
    public Message processing(Message message, Rule[] rules) {

        try {
            SortedMap<String, String> ruleList = new TreeMap<>();
            Long maxKey = 0L;
            JsonNode jsonNode = mapper.readTree(message.getValue());
            for (var rule : rules) {
                if (Boolean.FALSE.equals(rule.getIsActive()))
                    continue;
                JsonNode temp = jsonNode.get(rule.getFieldName());
                if (temp == null)
                {
                    message.setDeduplicationState(false);
                    return message;
                }

                if (!temp.isValueNode())
                {
                    message.setDeduplicationState(false);
                    return message;
                }

                String value = jsonNode.get(rule.getFieldName()).asText();
                if (value == null)
                {
                    message.setDeduplicationState(false);
                    return message;
                }
                if (value.isEmpty())
                {
                    message.setDeduplicationState(false);
                    return message;
                }
                if (rule.getTimeToLiveSec() > maxKey)
                    maxKey = rule.getTimeToLiveSec();
                ruleList.put(rule.getFieldName(), value);
            }

            String joinedList = ruleList.entrySet()
                    .stream()
                    .map(el -> el.getKey() + el.getValue())
                    .collect(Collectors.joining("*", "{", "}"));

            if (redisClient.findKey(joinedList)) {
                log.info("duplex msg: rule: {}", joinedList);
                message.setDeduplicationState(false);
                return message;
            }

            redisClient.insert(joinedList, joinedList, maxKey);
            message.setDeduplicationState(true);
            return message;

        } catch (Exception e){
            log.info("Json error :{}", e.toString());
        }

        return message;
    }
}
