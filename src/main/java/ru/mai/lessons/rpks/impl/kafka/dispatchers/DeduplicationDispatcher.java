package ru.mai.lessons.rpks.impl.kafka.dispatchers;

import lombok.extern.slf4j.Slf4j;
import org.json.JSONException;
import org.json.JSONObject;
import ru.mai.lessons.rpks.exceptions.UndefinedOperationException;
import ru.mai.lessons.rpks.impl.kafka.KafkaWriterImpl;
import ru.mai.lessons.rpks.impl.repository.RulesUpdaterThread;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class DeduplicationDispatcher implements DispatcherKafka {
    private final String topicToSendMsg;
    private final String bootstrapServers;
    private final RulesUpdaterThread updaterRulesThread;
    private ConcurrentHashMap<String, List<Rule>> rulesConcurrentMap;
    private final KafkaWriterImpl kafkaWriter;

    public DeduplicationDispatcher(String topicToSendMsg, String kafkaTopicBootstrap, RulesUpdaterThread updaterRulesThread) {
        this.topicToSendMsg = topicToSendMsg;
        this.bootstrapServers = kafkaTopicBootstrap;
        this.updaterRulesThread = updaterRulesThread;
        updateRules();
        kafkaWriter = createKafkaWriterForSendingMessage();
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
//        updateRules();
//        log.info("Size of rules: {}", rulesConcurrentMap.size());
//        if (rulesConcurrentMap.size() == 0) {
//            kafkaWriter.processing(getMessage(checkingMessage, false));
//            return;
//        }
//        try {
//            JSONObject jsonObject = new JSONObject(checkingMessage);
//
//            boolean isCompatible = checkField("name", jsonObject);
//            log.info("compatible after name: {}", isCompatible);
//            if (!isCompatible) {
//                kafkaWriter.processing(getMessage(checkingMessage, false));
//                return;
//            }
//            isCompatible = checkField("age", jsonObject);
//            log.info("compatible after age: {}", isCompatible);
//            if (!isCompatible) {
//                kafkaWriter.processing(getMessage(checkingMessage, false));
//                return;
//            }
//
//            isCompatible = checkField("sex", jsonObject);
//            log.info("compatible after sex: {}", isCompatible);
//            if (!isCompatible) {
//                kafkaWriter.processing(getMessage(checkingMessage, false));
//                return;
//            }
//            kafkaWriter.processing(getMessage(checkingMessage, true));
//        } catch (JSONException ex) {
//            log.error("Parsing json error: ", ex);
//        }
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
//        return Message.builder()
//                .value(value)
//                .filterState(isCompatible)
//                .build();
        return null;
    }

    private KafkaWriterImpl createKafkaWriterForSendingMessage() {
//        return KafkaWriterImpl.builder()
//                .topic(topicToSendMsg)
//                .bootstrapServers(bootstrapServers)
//                .build();
        return null;
    }
}
