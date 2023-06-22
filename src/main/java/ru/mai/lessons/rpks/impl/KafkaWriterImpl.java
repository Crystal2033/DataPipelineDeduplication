package ru.mai.lessons.rpks.impl;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import ru.mai.lessons.rpks.KafkaWriter;
import ru.mai.lessons.rpks.model.Message;

import java.util.Properties;

@Slf4j
public class KafkaWriterImpl implements KafkaWriter {
    private final String topic;
    private KafkaProducer<String, String> producer;

    public KafkaWriterImpl(String topic, String bootstrapServers){
        this.topic = topic;
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        producer = new KafkaProducer<>(properties);
    }

    @Override
    public void processing(Message message) {
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>(topic, message.getValue());
        producer.send(producerRecord);
        producer.flush();
    }
}
