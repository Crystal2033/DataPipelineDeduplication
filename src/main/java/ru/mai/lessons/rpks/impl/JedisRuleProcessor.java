package ru.mai.lessons.rpks.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.sun.source.tree.Tree;
import com.typesafe.config.Config;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.jooq.tools.StringUtils;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.util.*;

@Setter
@Getter
@Slf4j
public class JedisRuleProcessor implements RuleProcessor {

    private RedisClientRealization redisClient;

    ObjectMapper mapper = new ObjectMapper();
    @Override
    public Message processing(Message message, Rule[] rules) {

        try {
            List<String> newRule = new ArrayList<>();
            Long time = 0L;

            Map<String, String> map;
            map = mapper.readValue(message.getValue(), new TypeReference<Map<String, String>>() {
            });

            if (checkMessage(map)) {
                message.setDeduplicationState(false);
                return message;
            }

            for (Rule rule : rules) {
                if (Boolean.TRUE.equals(rule.getIsActive())) {
                    newRule.add(rule.getFieldName());
                    if (rule.getTimeToLiveSec() > time)
                        time = rule.getTimeToLiveSec();
                }
            }


            TreeMap<String, String> newJson = new TreeMap<>();
            for (String name : newRule) {
                if (map.containsKey(name)) {
                    newJson.put(name, map.get(name));
                }
            }

            String key = mapper.writeValueAsString(newJson);

            if (newJson.size() == 0) {
                message.setDeduplicationState(true);
                return message;
            }


            message.setDeduplicationState(true);
            redisClient.insert(key, time);
            return message;
        }
        catch (JsonProcessingException e) {
            log.info("Message {} have uncorrected data", message.getValue());
            message.setDeduplicationState(false);
            return message;
        }
    }

    boolean checkMessage(Map<String, String> messageMap) throws JsonProcessingException {
        Set<String> keys;
        if (redisClient.getKeys("*").isEmpty())
            keys = redisClient.getKeys("*");
        else
            return false;

        for (String key : keys) {
            Map<String, String> map;
            map = mapper.readValue(key, new TypeReference<Map<String, String>>() {
            });

            boolean flag = true;
            for (String it : map.keySet()) {
                if (!Objects.equals(map.get(it), messageMap.get(it))) {
                    flag = false;
                }
            }

            if (Boolean.TRUE.equals(flag))
                return true;
        }
        return false;
    }

    public void createRedisClient(Config config) {
        this.redisClient = new RedisClientRealization(config);
    }
}
