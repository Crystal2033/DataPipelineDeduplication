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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import ru.mai.lessons.rpks.KafkaReader;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@Slf4j
@Getter
@Setter
@RequiredArgsConstructor
public class KafkaReaderImpl implements KafkaReader {

    private final String topic;
    private final String topicOut;
    private final String bootstrapServers;
    private final String bootstrapServersWriter;
    @NonNull
    Rule[] rules;
    @NonNull
    Config config;
    private boolean isExit;

    public void processing() {
        int updateIntervalSec = config.getInt("application.updateIntervalSec");
        Db db = new Db(config);
        rules = db.readRulesFromDB();
        log.info("Start reading kafka topic {}", topic);
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                        ConsumerConfig.GROUP_ID_CONFIG, "tc-" + UUID.randomUUID(),
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
                ),
                new StringDeserializer(),
                new StringDeserializer()
        );
        TimerTask task = new TimerTask() {
            public void run() {
                rules = db.readRulesFromDB();
            }
        };

        Timer timer = new Timer(true);

        timer.schedule(task, 0, 1000L * updateIntervalSec);


        kafkaConsumer.subscribe(Collections.singletonList(topic));
        try (kafkaConsumer) {
            while (!isExit) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords)
                {
                    kafkaSend(consumerRecord);
                }
            }
            log.info("Read is done!");

        }
    }
    private void kafkaSend(ConsumerRecord<String, String> consumerRecord){

        if (consumerRecord.value().equals("$exit")) {
            isExit = true;
        }
        log.info("Message from Kafka topic {} : {}", consumerRecord.topic(), consumerRecord.value());
        Message msg = new Message(consumerRecord.value(), true);
        RuleProcessorImpl ruleProcessor = new RuleProcessorImpl(config);
        Message processedMsg = ruleProcessor.processing(msg, rules);
        log.info("Start write message in kafka out topic {}", topicOut);
        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServersWriter,
                        ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString()
                ),
                new StringSerializer(),
                new StringSerializer()
        )) {
            if (processedMsg != null) {
                Future<RecordMetadata> response = null;

                if (processedMsg.isDeduplicationState()) {
                    if (Objects.equals(processedMsg.getValue(), "$exit")) {
                        isExit = true;
                        return;
                    }
                    response = kafkaProducer.send(new ProducerRecord<>(topicOut, processedMsg.getValue()));
                    Optional.ofNullable(response).ifPresent(rsp -> {
                        try {
                            log.info("Message send to out{}", rsp.get());
                        } catch (InterruptedException | ExecutionException e) {
                            log.error("Error sending message ", e);
                            Thread.currentThread().interrupt();
                        }
                    });
                }
            }
        }catch (KafkaException e) {
            log.error("kafka exception caught");
        }

    }
}