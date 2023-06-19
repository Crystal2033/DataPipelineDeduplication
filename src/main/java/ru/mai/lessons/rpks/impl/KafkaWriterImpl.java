package ru.mai.lessons.rpks.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import ru.mai.lessons.rpks.KafkaWriter;
import ru.mai.lessons.rpks.model.Message;

import java.util.Map;
import java.util.UUID;

@Slf4j
@RequiredArgsConstructor
public final class KafkaWriterImpl implements KafkaWriter {
    private KafkaProducer<String, String> kafkaProducer;
    private final String topic;
    private final String bootstrapServers;

    @Override
    public void processing(Message message) {
        send(message);
    }

    private void send(Message message){
        if (kafkaProducer == null)
            createProducer();

        kafkaProducer.send(new ProducerRecord<>(topic, message.getValue()));
    }

    private void createProducer() {
        log.info("Create kafka writer");

        kafkaProducer = new KafkaProducer<>(
                Map.of(
                        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                        ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString()
                ),
                new StringSerializer(),
                new StringSerializer()
        );
    }
}