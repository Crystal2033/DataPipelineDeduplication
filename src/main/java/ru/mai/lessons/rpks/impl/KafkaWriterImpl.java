package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.NonNull;
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

public class KafkaWriterImpl implements KafkaWriter {
    private final String topic;
    @NonNull
    Config config;
    KafkaProducer<String, String> kafkaProducer;
    public KafkaWriterImpl(Config configIn) {
        config = configIn;
        kafkaProducer = new KafkaProducer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, configIn.getString("kafka.producer.bootstrap.servers"),
                ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString()
        ),
                new StringSerializer(),
                new StringSerializer()
        );
        topic = configIn.getString("kafka.topic_out");
    }
    @Override
    public void processing(Message message) {
        log.debug("Start write message in kafka topic {}", topic);
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, message.getValue());
        kafkaProducer.send(producerRecord);
        log.debug("Message send {}", message.getValue());
    }
}
