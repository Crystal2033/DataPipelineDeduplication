package ru.mai.lessons.rpks.impl;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;
import com.typesafe.config.Config;

import java.util.Map;

@Slf4j
@RequiredArgsConstructor
public class RuleProcessorImpl implements RuleProcessor {
    @NonNull
    Config config;
    long longerTime = 0L;
    @Override
    public Message processing(Message message, Rule[] rules) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_ABSENT);
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        mapper.configure(DeserializationFeature.FAIL_ON_MISSING_CREATOR_PROPERTIES, false);

        String host = config.getConfig("redis").getString("host");
        int port = config.getConfig("redis").getInt("port");
        RedisClientImpl redisClient = new RedisClientImpl(host, port);

        try {
            longerTime = 0L;
            StringBuilder ruleForRedis = new StringBuilder();
            Map<String, Object> msg = mapper.readValue(message.getValue(), Map.class);
            if (rules.length != 0) {
                for (Rule rule: rules) {
                    if (msg.containsKey(rule.getFieldName())) {
                        createRuleForRedis(msg, rule, ruleForRedis);
                    }
                    else {
                        message.setDeduplicationState(false);
                        break;
                    }
                }
                if (!ruleForRedis.isEmpty()) {
                    message.setDeduplicationState(redisClient.isExist(ruleForRedis.toString(), longerTime));
                }
            }
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            message.setDeduplicationState(false);
        } catch (Exception e) {
            e.printStackTrace();
            message.setDeduplicationState(false);
        }
        return message;
    }

    void createRuleForRedis(Map<String, Object> msg, Rule rule, StringBuilder str) {
        if (rule.getTimeToLiveSec() > longerTime) {
            longerTime = rule.getTimeToLiveSec();
        }
        str.append(msg.get(rule.getFieldName()));
        str.append(";");
    }
}
