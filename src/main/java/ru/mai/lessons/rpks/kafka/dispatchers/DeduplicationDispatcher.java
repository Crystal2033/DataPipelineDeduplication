package ru.mai.lessons.rpks.kafka.dispatchers;

import lombok.extern.slf4j.Slf4j;
import org.json.JSONException;
import org.json.JSONObject;
import ru.mai.lessons.rpks.exceptions.UndefinedOperationException;
import ru.mai.lessons.rpks.kafka.impl.KafkaWriterImpl;
import ru.mai.lessons.rpks.kafka.interfaces.DispatcherKafka;
import ru.mai.lessons.rpks.redis.interfaces.RedisClient;
import ru.mai.lessons.rpks.repository.impl.RulesUpdaterThread;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static ru.mai.lessons.rpks.constants.MainNames.*;

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
    public void actionWithMessage(String msg) throws UndefinedOperationException {
        sendMessageIfCompatibleWithDBRules(msg);
    }

    public void closeReadingThread() {
        updaterRulesThread.stopReadingDataBase();
    }


    private boolean checkField(String fieldName, JSONObject jsonObject) throws UndefinedOperationException {
//        if (rulesConcurrentMap.containsKey(fieldName)) {
//            String userValue = jsonObject.get(fieldName).toString();
//            List<Rule> rules = rulesConcurrentMap.get(fieldName);
//            for (var rule : rules) {
//                if (!isCompatibleWithRule(rule.getFilterFunctionName(), rule.getFilterValue(), userValue)) {
//                    return false;
//                }
//            }
//            return true;
//        }
//        return true;
        return true;
    }

    private void sendMessageIfCompatibleWithDBRules(String checkingMessage) throws UndefinedOperationException {
        updateRules();
        log.info("Size of rules: {}", rulesConcurrentMap.size());
        if (rulesConcurrentMap.size() == 0) {
            kafkaWriter.processing(getMessage(checkingMessage, true));
            return;
        }
        kafkaWriter.processing(checkForDuplicateAndGetMessageByString(checkingMessage));
    }

    private String getKeyByFields(String checkingMessage){
        StringBuilder keyBuilder = new StringBuilder();
        try {
            JSONObject jsonObject = new JSONObject(checkingMessage);

            if(rulesConcurrentMap.containsKey(NAME_STRING_VALUE)){
                appendNewValueInKey(keyBuilder, jsonObject.get(NAME_STRING_VALUE).toString());
            }
            if(rulesConcurrentMap.containsKey(AGE_STRING_VALUE)){
                appendNewValueInKey(keyBuilder, jsonObject.get(AGE_STRING_VALUE).toString());
            }
            if(rulesConcurrentMap.containsKey(SEX_STRING_VALUE)){
                appendNewValueInKey(keyBuilder, jsonObject.get(SEX_STRING_VALUE).toString());
            }
        }
        catch (JSONException ex) {
            log.error("Parsing json error: ", ex);
        }
        return keyBuilder.toString();// TODO: probably null;
    }



    private void appendNewValueInKey(StringBuilder keyBuilder, String newValue){
        keyBuilder.append(newValue);
        keyBuilder.append(":");
    }
    private Message checkForDuplicateAndGetMessageByString(String checkingMessage){
        long expireTimeInSec = getTotalExpireTimeByExistingRules();
        return redis.getMessageByStringAndTryToInsertInRedis(checkingMessage, getKeyByFields(checkingMessage), expireTimeInSec);
    }

    private long getTotalExpireTimeByExistingRules(){
        long expireTimeInSec = 0;
        List<Rule> rules = rulesConcurrentMap.get(NAME_STRING_VALUE);
        expireTimeInSec = getMaxExpireTimeByField(rules, expireTimeInSec);

        rules = rulesConcurrentMap.get(AGE_STRING_VALUE);
        expireTimeInSec = getMaxExpireTimeByField(rules, expireTimeInSec);

        rules = rulesConcurrentMap.get(SEX_STRING_VALUE);
        expireTimeInSec = getMaxExpireTimeByField(rules, expireTimeInSec);

        return expireTimeInSec;

    }

    private long getMaxExpireTimeByField(List<Rule> rules, long currentExpireTime){
        if(!rules.isEmpty()){
            return Math.max(currentExpireTime, rules.stream().max(
                    Comparator.comparing(Rule::getTimeToLiveSec)).get().getTimeToLiveSec()
            );
        }
        else{
            return currentExpireTime;
        }
    }
    private boolean isCompatibleWithRule(String operation, String expected, String userValue) throws UndefinedOperationException {
//        log.info("operation={}, expected={}, userValue={}", operation, expected, userValue);
//        return switch (operation) {
//            case "equals" -> expected.equals(userValue);
//            case "not_equals" -> !expected.equals(userValue);
//            case "contains" -> userValue.contains(expected);
//            case "not_contains" -> !userValue.contains(expected);
//            default -> throw new UndefinedOperationException("Operation was not found.", operation);
//        };
        return false;
    }

    private Message getMessage(String value, boolean isCompatible) {
        return Message.builder()
                .value(value)
                .deduplicationState(isCompatible)
                .build();
    }
}
