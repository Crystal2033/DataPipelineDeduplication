package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import ru.mai.lessons.rpks.KafkaWriter;
import ru.mai.lessons.rpks.model.Message;

import java.util.Properties;

@Slf4j
public class KafkaWriterRealization implements KafkaWriter {

    String topic;
    KafkaProducer<String, String> producer;
    @Override
    public void processing(Message message) {
        log.info("send message - {}", message.getValue());
        producer.send(new ProducerRecord<>(topic, message.getValue()));
    }

    public void createProducer(Config config) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", config.getString("kafka.producer.bootstrap.servers"));
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        topic = config.getString("kafka.producer.topic");
        producer = new KafkaProducer<>(properties);
    }
}
