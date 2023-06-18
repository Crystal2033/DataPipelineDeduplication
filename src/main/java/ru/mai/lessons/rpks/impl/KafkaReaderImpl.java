package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import ru.mai.lessons.rpks.DbReader;
import ru.mai.lessons.rpks.DeduplicationRedisService;
import ru.mai.lessons.rpks.KafkaReader;
import ru.mai.lessons.rpks.KafkaWriter;
import ru.mai.lessons.rpks.config.KafkaConfig;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.time.Duration;
import java.util.Collections;
import java.util.stream.StreamSupport;

@Slf4j
public class KafkaReaderImpl implements KafkaReader {

    private final KafkaConsumer<String, String> consumer;
    private final KafkaWriter kafkaWriter;
    private final DbReader dbReader;
    private final DeduplicationRedisService deduplicationService;

    public KafkaReaderImpl(Config config) {
        this.consumer = KafkaConfig.createConsumer(config);
        consumer.subscribe(Collections.singletonList(KafkaConfig.getTopicIn(config)));
        this.kafkaWriter = new KafkaWriterImpl(config);
        this.dbReader = new DbReaderImpl(config);
        this.deduplicationService = new DeduplicationRedisServiceImpl(config);
    }


    @Override
    public void processing()
    {
        while (true) {
            log.debug("Attempt to read");
            if (Thread.interrupted()) {
                break;
            }
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            log.debug("Got records " + records.count());
            Rule[] rules = dbReader.readRulesFromDB();
            StreamSupport.stream(records.spliterator(), false)
                    .peek(r -> log.debug("Got record {}", r.value()))
                    .forEach(r -> kafkaWriter.processing(Message.builder()
                            .value(r.value())
                            .deduplicationState(deduplicationService.deduplicate(r, rules))
                            .build()));
        }
    }
}