package ru.mai.lessons.rpks.impl;


import lombok.Setter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import ru.mai.lessons.rpks.KafkaWriter;
import ru.mai.lessons.rpks.model.Message;

import java.util.Map;
import java.util.Optional;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.*;

@Slf4j
@RequiredArgsConstructor
@Setter

public class KafkaWriterImpl implements KafkaWriter {
    private final String topic;
    private final String bootstrapServers;
    @Override
    public void processing(Message message) {

        log.info("Start console write message in kafka topic {}", topic);
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                        ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString()
                ),
                new StringSerializer(),
                new StringSerializer()
        );

        try (kafkaProducer;
             Scanner scanner = new Scanner(System.in)) {
            String inputData;


            do {
                inputData = scanner.nextLine();
                Future<RecordMetadata> response = null;
                response = kafkaProducer.send(new ProducerRecord<>(topic, inputData));
                Optional.ofNullable(response).ifPresent(rsp -> {
                    try {
                        log.info("Message send {}", rsp.get());
                    } catch (InterruptedException | ExecutionException e) {
                        log.error("Error sending message ", e);
                        Thread.currentThread().interrupt();
                    }
                });
            } while (!inputData.equals("$exit"));

        }
    }


}
