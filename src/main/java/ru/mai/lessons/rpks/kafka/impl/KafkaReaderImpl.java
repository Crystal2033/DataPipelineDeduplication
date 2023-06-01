package ru.mai.lessons.rpks.kafka.impl;

import com.typesafe.config.Config;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import ru.mai.lessons.rpks.configs.ConfigurationReader;
import ru.mai.lessons.rpks.configs.interfaces.ConfigReader;
import ru.mai.lessons.rpks.dispatchers.DeduplicationDispatcher;
import ru.mai.lessons.rpks.exceptions.ThreadWorkerNotFoundException;
import ru.mai.lessons.rpks.kafka.interfaces.KafkaReader;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
@Getter
@Setter
@Builder
@AllArgsConstructor
public class KafkaReaderImpl implements KafkaReader {
    private final String topic;
    private final String groupId;
    private final String autoOffsetReset;
    private final String bootstrapServers;

    private final String exitWord;

    private final DeduplicationDispatcher dispatcherKafka;

    private List<KafkaConsumer<String, String>> kafkaConsumers;


    @Override
    public void processing() {
        ConfigReader configurationReader = new ConfigurationReader();
        Config config = configurationReader.loadConfig().getConfig("kafka").getConfig("consumer");
        int valueOfThreads = config.getInt("threads");

        kafkaConsumers = new ArrayList<>();
        ExecutorService executorService = Executors.newFixedThreadPool(valueOfThreads);
        for (int i = 0; i < valueOfThreads; i++) {
            KafkaConsumer<String, String> kafkaConsumer = initKafkaConsumer();
            kafkaConsumers.add(kafkaConsumer);
            kafkaConsumer.subscribe(Collections.singletonList(topic));
            if (i != valueOfThreads - 1) {
                executorService.execute(() -> listenAndDelegateWork(kafkaConsumer));
            }
        }
        listenAndDelegateWork(kafkaConsumers.get(valueOfThreads - 1));
        executorService.shutdown();
    }

    private void listenAndDelegateWork(KafkaConsumer<String, String> kafkaConsumer) {
        try (kafkaConsumer) {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    log.info("Message from Kafka topic {} : {}", consumerRecord.topic(), consumerRecord.value());
                    sendForDeduplication(consumerRecord.value());
                }
            }
        }
    }

    private KafkaConsumer<String, String> initKafkaConsumer() {
        return new KafkaConsumer<>(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                        ConsumerConfig.GROUP_ID_CONFIG, "groupId: " + groupId,
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset
                ),
                new StringDeserializer(),
                new StringDeserializer()
        );
    }

    private void sendForDeduplication(String msg) {
        try {
            dispatcherKafka.actionWithMessage(msg);
        } catch (ThreadWorkerNotFoundException e) {
            log.error("There is a problem with thread worker: " + e.getMessage());
        }
    }
}
