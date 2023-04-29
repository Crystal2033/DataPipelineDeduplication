package ru.mai.lessons.rpks.kafka.dispatchers;

import lombok.extern.slf4j.Slf4j;
import org.json.JSONException;
import org.json.JSONObject;
import ru.mai.lessons.rpks.kafka.impl.KafkaWriterImpl;
import ru.mai.lessons.rpks.kafka.interfaces.DispatcherKafka;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;
import ru.mai.lessons.rpks.redis.interfaces.RedisClient;
import ru.mai.lessons.rpks.repository.impl.RulesUpdaterThread;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class DeduplicationDispatcher implements DispatcherKafka {
    private final RulesUpdaterThread updaterRulesThread; //to get actual rules, which are in db thread reader
    private ConcurrentHashMap<String, List<Rule>> rulesConcurrentMap;
    private final KafkaWriterImpl kafkaWriter;
    private final RedisClient redis;

    public DeduplicationDispatcher(KafkaWriterImpl kafkaWriter, RedisClient redis, RulesUpdaterThread updaterRulesThread) {
        this.updaterRulesThread = updaterRulesThread;
        updateRules();
        this.kafkaWriter = kafkaWriter;
        this.redis = redis;
    }

    public void updateRules() {
        if (updaterRulesThread != null) {
            rulesConcurrentMap = updaterRulesThread.getRulesConcurrentMap();
        }
    }

    @Override
    public void actionWithMessage(String msg) {
        sendMessageIfCompatibleWithDBRules(msg);
    }

    public void closeReadingThread() {
        updaterRulesThread.stopReadingDataBase();
    }

    private void sendMessageIfCompatibleWithDBRules(String checkingMessage) {
        updateRules();
        if (rulesConcurrentMap.size() == 0) {
            kafkaWriter.processing(getMessage(checkingMessage, false));
        } else {
            Optional<Message> optionalMessage = Optional.ofNullable(checkForDuplicateAndGetMessageByString(checkingMessage));
            optionalMessage.ifPresent(kafkaWriter::processing);
        }

    }

    private String getKeyByFields(String checkingMessage) {
        StringBuilder keyBuilder = new StringBuilder();
        try {
            JSONObject jsonObject = new JSONObject(checkingMessage);
            for (String key : jsonObject.keySet()) {
                if (rulesConcurrentMap.containsKey(key)) {
                    appendNewValueInKey(keyBuilder, jsonObject.get(key).toString());
                }
            }
        } catch (JSONException ex) {
            log.error("Parsing json message {} error: ", checkingMessage, ex);
            return null;
        }
        return keyBuilder.toString();
    }


    private void appendNewValueInKey(StringBuilder keyBuilder, String newValue) {
        if (!keyBuilder.isEmpty()) {
            keyBuilder.append(":");
        }
        keyBuilder.append(newValue);
    }

    private Message checkForDuplicateAndGetMessageByString(String checkingMessage) {
        long expireTimeInSec = getTotalExpireTimeByExistingRules();
        Optional<String> optionalRedisKey = Optional.ofNullable(getKeyByFields(checkingMessage));
        if (optionalRedisKey.isPresent()) {
            Message message = redis.getMessageByStringAndTryToInsertInRedis(checkingMessage, optionalRedisKey.get(), expireTimeInSec);
            return message;
        } else {
            return null;
        }

    }

    private long getTotalExpireTimeByExistingRules() {
        long expireTimeInSec = 0;
        for (var listOfRules : rulesConcurrentMap.values()) {
            expireTimeInSec = getMaxExpireTimeByField(listOfRules, expireTimeInSec);
        }
        return expireTimeInSec;
    }

    private long getMaxExpireTimeByField(List<Rule> rules, long currentExpireTime) {
        if (rules != null) {
            Optional<Rule> ruleWithMaxExpireTime = rules.stream()
                    .max(Comparator.comparing(Rule::getTimeToLiveSec));
            return Math.max(currentExpireTime,
                    ruleWithMaxExpireTime.isEmpty() ? 0 : ruleWithMaxExpireTime.get().getTimeToLiveSec());
        } else {
            return currentExpireTime;
        }
    }

    private Message getMessage(String value, boolean isDuplicate) {
        return Message.builder()
                .value(value)
                .isDuplicate(isDuplicate)
                .build();
    }
}
