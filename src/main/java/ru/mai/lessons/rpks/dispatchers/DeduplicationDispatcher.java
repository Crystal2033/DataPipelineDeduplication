package ru.mai.lessons.rpks.dispatchers;

import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.kafka.impl.KafkaWriterImpl;
import ru.mai.lessons.rpks.kafka.interfaces.DispatcherKafka;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;
import ru.mai.lessons.rpks.processors.interfaces.RuleProcessor;
import ru.mai.lessons.rpks.repository.impl.RulesUpdaterThread;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class DeduplicationDispatcher implements DispatcherKafka {
    private final RulesUpdaterThread updaterRulesThread; //to get actual rules, which are in db thread reader
    private ConcurrentHashMap<String, List<Rule>> rulesConcurrentMap;
    private final KafkaWriterImpl kafkaWriter;

    private final RuleProcessor ruleProcessor;

    public DeduplicationDispatcher(KafkaWriterImpl kafkaWriter, RuleProcessor ruleProcessor,
                                   RulesUpdaterThread updaterRulesThread) {
        this.updaterRulesThread = updaterRulesThread;
        updateRules();
        this.kafkaWriter = kafkaWriter;
        this.ruleProcessor = ruleProcessor;
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
            Optional<Message> optionalMessage = Optional.ofNullable(
                    ruleProcessor.processing(getMessage(checkingMessage, false), rulesConcurrentMap));
            optionalMessage.ifPresent(kafkaWriter::processing);
        }

    }


    private Message getMessage(String value, boolean isDuplicate) {
        return Message.builder()
                .value(value)
                .isDuplicate(isDuplicate)
                .build();
    }
}
