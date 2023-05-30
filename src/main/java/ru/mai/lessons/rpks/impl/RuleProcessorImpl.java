package ru.mai.lessons.rpks.impl;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.typesafe.config.Config;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.util.Map;
import java.util.Objects;

@Slf4j
@RequiredArgsConstructor
public class RuleProcessorImpl implements RuleProcessor {
    boolean isExit = false;
    private RedisClientImpl redisClient;
    @NonNull
    Config config;
    ObjectMapper mapper;
    @Override
    public Message processing(Message message, Rule[] rules) {
        mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_ABSENT);
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        mapper.configure(DeserializationFeature.FAIL_ON_MISSING_CREATOR_PROPERTIES, false);
        String host = config.getConfig("redis").getString("host");
        int port = config.getConfig("redis").getInt("port");
        redisClient = new RedisClientImpl(host, port);
        if (Objects.equals(message.getValue(), "$exit")) {
            isExit = true;
        }
        message.setDeduplicationState(true);


        try{
            long timeToLive = -1;
            String key = "";
            StringBuilder str = new StringBuilder();
            Map<String, Object> map = mapper.readValue(message.getValue(), Map.class);
            if (!isExit) {
                if (rules.length == 0) {
                    message.setDeduplicationState(true);
                    return message;
                }
                for (Rule rule : rules) {
                    //            ПРОВЕРКА ПРАВИЛ
                    log.info("RULES LENGTH {}", rules.length);
                    log.info("CHECKING FIELD {}", rule.getFieldName());
                    if (map.containsKey(rule.getFieldName())) {
                        if (rule.getIsActive()) {
                            if (rule.getTimeToLiveSec() > timeToLive) {
                                timeToLive = rule.getTimeToLiveSec();
                            }

                            str.append("_");
                            str.append(map.get(rule.getFieldName()));
                        }
                    } else {
                        message.setDeduplicationState(false);
                        return message;
                    }

                }
                key = str.toString();
                if (!key.isEmpty()){
                    message.setDeduplicationState(redisClient.existsKey(key, timeToLive));
                }

            } else {
                message.setValue("$exit");
                message.setDeduplicationState(true);
            }
        } catch (JsonMappingException e) {
            log.error("mapping exception caught");
            message.setDeduplicationState(false);
        } catch (JsonProcessingException e) {
            log.error("exception caught");
            message.setDeduplicationState(false);
        }
        catch (Exception e) {
            log.info("caught null exception");
            message.setDeduplicationState(false);
        }

        return message;
    }
}
