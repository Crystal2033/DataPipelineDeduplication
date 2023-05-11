package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.jooq.tools.json.*;
import ru.mai.lessons.rpks.ConfigReader;
import ru.mai.lessons.rpks.DbReader;
import ru.mai.lessons.rpks.KafkaReader;
import ru.mai.lessons.rpks.KafkaWriter;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.time.Duration;
import java.util.*;

@Slf4j
@Getter
@Setter
@RequiredArgsConstructor
public class KafReader implements KafkaReader {
    private final Config config;
    private final Queue<Rule[]> queue;
    private boolean isExit;

    public void processing() {
        // Считываем параметры конфигурации для подключения к Kafka
        String consumerServers = config.getString("kafka.consumer.bootstrap.servers");
        String group = config.getString("kafka.consumer.group.id");
        String offsetReset = config.getString("kafka.consumer.auto.offset.reset");
        String topicIn = config.getString("kafka.consumer.topicIn");
        String producerServers = config.getString("kafka.producer.bootstrap.servers");
        String topicOut = config.getString("kafka.producer.topicOut");

        // Считываем параметры конфигурации для подключения к Redis
        String host = config.getString("redis.host");
        int port = config.getInt("redis.port");

        RedClient redisClient = new RedClient(host, port);
        RuleProcess ruleProcess = new RuleProcess(redisClient);
        KafkaWriter kafkaWriter = new KafWriter(topicOut, producerServers);

        log.info("Start reading kafka topic {}", topicIn);
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, consumerServers,
                        ConsumerConfig.GROUP_ID_CONFIG, group,
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetReset
                ),
                new StringDeserializer(),
                new StringDeserializer()
        );

        kafkaConsumer.subscribe(Collections.singletonList(topicIn));

        try (kafkaConsumer) {
            while (!isExit) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));

                Rule[] rules = queue.peek();
                assert rules != null;
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    String jsonString =  consumerRecord.value();
                    if (jsonString.isBlank()) continue;

                    Message message = Message.builder().value(jsonString).deduplicationState(true).build();
                    message = ruleProcess.processing(message, rules); // проверка сообщения

                    if(message.isDeduplicationState()){
                        kafkaWriter.processing(message);
                    }
                }
            }
            log.info("Read is done!");
        } catch (ParseException e) {
            log.warn("ParseException!", e);
        }
    }
}
