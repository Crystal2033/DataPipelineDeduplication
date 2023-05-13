package ru.mai.lessons.rpks.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import ru.mai.lessons.rpks.KafkaWriter;
import ru.mai.lessons.rpks.model.Message;
import java.util.Map;
import java.util.UUID;

@Slf4j
@RequiredArgsConstructor
public class KafWriter implements KafkaWriter {
    private final String topic;
    private final KafkaProducer<String, String> kafkaProducer;

    public KafWriter(String topic, String bootstrapServers) {
        this.topic = topic;

        this.kafkaProducer = new KafkaProducer<>(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                        ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString()
                ),
                new StringSerializer(),
                new StringSerializer()
        );
    }

    public void processing(Message message) {
        kafkaProducer.send(new ProducerRecord<>(topic, message.getValue()));
    }
}
