package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import ru.mai.lessons.rpks.KafkaWriter;
import ru.mai.lessons.rpks.model.Message;

import java.util.Properties;

public class KafkaWriterI implements KafkaWriter {
    KafkaProducer<String, String> producer;
    String topic;

    public KafkaWriterI(Config conf){
        Properties props = new Properties();
        props.put("bootstrap.servers", conf.getConfig("producer").getString("bootstrap.servers"));
        producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
        topic = conf.getConfig("producer").getString("topic");
    }
    @Override
    public void processing(Message message) {
        if (message.isDeduplicationState())
            producer.send(new ProducerRecord<>(topic, message.getValue()));
    }
}