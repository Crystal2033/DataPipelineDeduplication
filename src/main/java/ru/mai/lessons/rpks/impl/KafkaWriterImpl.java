package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.NonNull;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import ru.mai.lessons.rpks.KafkaWriter;
import ru.mai.lessons.rpks.model.Message;

import java.util.Map;
import java.util.UUID;



public class KafkaWriterImpl implements KafkaWriter {
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

    }

    @Override
    public void processing(Message message) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(
                config.getString("kafka.topicOut"),
                message.getValue()
        );

        kafkaProducer.send(producerRecord);

    }
}
