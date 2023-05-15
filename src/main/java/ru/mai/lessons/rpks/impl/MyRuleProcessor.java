package ru.mai.lessons.rpks.impl;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

@Slf4j
public class MyRuleProcessor implements RuleProcessor {


    MyRedisClient redisClient;
    ObjectMapper mapper;

    public MyRuleProcessor(Config config) {
        redisClient = new MyRedisClient(config);
        mapper = new ObjectMapper();
    }

    @Override
    public Message processing(Message message, Rule[] rules) {
        try {
            if (message.getValue().isEmpty()) {
                message.setDeduplicationState(false);
                return message;
            }

            if (rules.length == 0) {
                message.setDeduplicationState(true);
                return message;
            }

            JsonNode jsonNode = mapper.readTree(message.getValue());

            long time = -1;
            String value;
            StringBuilder stringBuilder = new StringBuilder();
            for (Rule rule : rules) {
                if (rule.isActive()) {
                    if (rule.getTimeToLiveSec() > time) {
                        time = rule.getTimeToLiveSec();
                    }

                    stringBuilder.append("_");
                    stringBuilder.append(jsonNode.get(rule.getFieldName()));
                }

            }

            value = stringBuilder.toString();
            if (!value.isEmpty())
                message.setDeduplicationState(redisClient.checkExist(value, time));

            return message;
        } catch (JsonProcessingException e) {
            log.error(e.getMessage());
            message.setDeduplicationState(false);
        }
        return message;
    }
}
