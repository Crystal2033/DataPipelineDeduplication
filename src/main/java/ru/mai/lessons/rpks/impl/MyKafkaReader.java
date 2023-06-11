package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.serialization.StringDeserializer;
import ru.mai.lessons.rpks.KafkaReader;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import ru.mai.lessons.rpks.KafkaWriter;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

@Slf4j
public class MyKafkaReader implements KafkaReader {
    private final KafkaConsumer<String, String> consumer;
    private final KafkaWriter kafkaWriter;
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();
    private final RuleProcessor ruleProcessor;
    private final Supplier<Rule[]> rulesSupplier;
    public MyKafkaReader(Config config, KafkaWriter kafkaWriter, RuleProcessor ruleProcessor, Supplier<Rule[]> rulesSupplier) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("bootstrap.servers"));
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, config.getString("group.id"));
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, config.getString("auto.offset.reset"));
        consumer = new KafkaConsumer<>(properties, new StringDeserializer(), new StringDeserializer());
        consumer.subscribe(Collections.singletonList(config.getString("topic")));
        log.info("READER -- Subscribe to topic {}", config.getString("topic"));
        log.info("READER -- Consumer group {}", config.getString("group.id"));
        log.info("READER -- Auto offset reset {}", config.getString("auto.offset.reset"));
        log.info("READER -- Bootstrap servers {}", config.getString("bootstrap.servers"));
        this.ruleProcessor = ruleProcessor;
        this.kafkaWriter = kafkaWriter;
        this.rulesSupplier = rulesSupplier;
    }
    @SuppressWarnings("InfiniteLoopStatement")
    @Override
    public void processing() {
        executorService.submit(() -> {
            while (true) {
                try {
                    ConsumerRecords<String, String> message = consumer.poll(Duration.ofMillis(100));
                    if (!message.isEmpty()) {
                        log.debug("READER -- Received {} messages", message.count());
                    }
                    Rule[] actualMessageRules = rulesSupplier.get();
                    for (var messageConsumerRecord : message) {
                        String messageValue = messageConsumerRecord.value();
                        kafkaWriter.processing(ruleProcessor.processing(Message.builder().value(messageValue).build(), actualMessageRules));
                    }
                } catch (InterruptException e) {
                    log.error("KAFKA READER -- Thread interrupted for some reason\nStack Trace:\n{}", Arrays.deepToString(e.getStackTrace()));
                    Thread.currentThread().interrupt();
                }
                catch (Exception e) {
                    log.error("KAFKA READER -- Caught an exception while reading Kafka\nStack Trace:\n{}", Arrays.deepToString(e.getStackTrace()));
                }
            }
        });
    }
}
