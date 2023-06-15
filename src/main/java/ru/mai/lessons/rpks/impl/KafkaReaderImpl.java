package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import ru.mai.lessons.rpks.KafkaReader;
import ru.mai.lessons.rpks.KafkaWriter;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

@Slf4j
@Getter
@Setter
@RequiredArgsConstructor
public class KafkaReaderImpl implements KafkaReader {
    private final String bootstrapServers;
    private final String topic;
    private boolean isExit;
    @NonNull
    Config config;
    @NonNull
    Rule[] rules;
    @Override
    public void processing() {
        log.info("Start reading kafka topic {}", topic);
        RuleProcessor ruleProcessor = new RuleProcessorImpl(config);
        KafkaWriter kafkaWriter = new KafkaWriterImpl(config);
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                        ConsumerConfig.GROUP_ID_CONFIG, "tc-" + UUID.randomUUID(),
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, config.getString("kafka.consumer.auto.offset.reset")
                ),
                new StringDeserializer(),
                new StringDeserializer()
        );
        kafkaConsumer.subscribe(Collections.singletonList(topic));

        try (kafkaConsumer) {
            while (!isExit) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    log.debug("Message from Kafka topic {} : {}", consumerRecord.topic(), consumerRecord.value());
                    Message msg = new Message(consumerRecord.value(), true);
                    Message checkMsg = ruleProcessor.processing(msg, rules);
                    if (checkMsg.isDeduplicationState()) {
                        kafkaWriter.processing(checkMsg);
                    }
                }
            }
        }
    }

    public void setRules(Rule[] rules) {
        this.rules = rules;
    }

}
