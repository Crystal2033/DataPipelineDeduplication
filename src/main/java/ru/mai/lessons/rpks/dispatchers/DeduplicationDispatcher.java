package ru.mai.lessons.rpks.dispatchers;

import com.typesafe.config.Config;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.exceptions.ThreadWorkerNotFoundException;
import ru.mai.lessons.rpks.kafka.impl.KafkaWriterImpl;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;
import ru.mai.lessons.rpks.processors.interfaces.RuleProcessor;
import ru.mai.lessons.rpks.repository.impl.RulesUpdaterThread;

import java.util.List;
import java.util.Map;
import java.util.Optional;

@Slf4j
@RequiredArgsConstructor
public class DeduplicationDispatcher {

    private static final String KAFKA_NAME = "kafka";
    private static final String TOPIC_NAME_PATH = "topic.name";
    private final Config config;
    private final RulesUpdaterThread updaterRulesThread; //to get actual rules, which are in db thread reader
    private KafkaWriterImpl kafkaWriter;

    private Map<String, List<Rule>> rulesMap;

    private final RuleProcessor ruleProcessor;

    public void updateRules() throws ThreadWorkerNotFoundException {
        rulesMap = Optional.ofNullable(updaterRulesThread).
                orElseThrow(() -> new ThreadWorkerNotFoundException("Database updater not found")).getRulesConcurrentMap();
    }


    public void actionWithMessage(String msg) throws ThreadWorkerNotFoundException {
        kafkaWriter = Optional.ofNullable(kafkaWriter).orElseGet(this::createKafkaWriterForSendingMessage);
        updateRules();
        if (rulesMap.isEmpty()) {
            kafkaWriter.processing(getMessage(msg, false));
        } else {
            Optional<Message> optionalMessage = Optional.ofNullable(
                    ruleProcessor.processing(getMessage(msg, false), rulesMap));
            optionalMessage.ifPresent(kafkaWriter::processing);
        }
    }

    private KafkaWriterImpl createKafkaWriterForSendingMessage() {
        Config producerKafkaConfig = config.getConfig(KAFKA_NAME).getConfig("producer");
        return KafkaWriterImpl.builder()
                .topic(producerKafkaConfig.getConfig("enrichment").getString(TOPIC_NAME_PATH))
                .bootstrapServers(producerKafkaConfig.getString("bootstrap.servers"))
                .build();
    }

    private Message getMessage(String value, boolean isDuplicate) {
        return Message.builder()
                .value(value)
                .isDuplicate(isDuplicate)
                .build();
    }
}
