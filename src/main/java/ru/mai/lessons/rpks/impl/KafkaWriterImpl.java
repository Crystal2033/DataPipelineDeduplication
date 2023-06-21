package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import ru.mai.lessons.rpks.KafkaWriter;
import ru.mai.lessons.rpks.model.Message;

import java.util.Properties;

public class KafkaWriterImpl implements KafkaWriter {
    private final KafkaProducer<String, String> producer;
    String topic;
    public KafkaWriterImpl(Config appConfig){
        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, appConfig.getString("kafka.producer.bootstrap.servers"));
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        topic = appConfig.getString("kafka.producer.topic");
        producer = new KafkaProducer<>(config);
    }
    @Override
    public void processing(Message message) {
        producer.send(new ProducerRecord<>(topic, message.getValue()));
    }
}
