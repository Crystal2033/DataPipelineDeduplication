package ru.mai.lessons.rpks.config;

import com.typesafe.config.Config;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;

@RequiredArgsConstructor
@Getter

public final class KafkaConfig {

    public static KafkaConsumer<String, String> createConsumer(Config config) {
        return new KafkaConsumer<>(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka.consumer.bootstrap.servers"),
                        ConsumerConfig.GROUP_ID_CONFIG, config.getString("kafka.consumer.group.id"),
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, config.getString("kafka.consumer.auto.offset.reset")
                ),
                new StringDeserializer(),
                new StringDeserializer()
        );
    }

    public static KafkaProducer<String, String> createProducer(Config config) {
        return new KafkaProducer<>(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka.producer.bootstrap.servers")
                ),
                new StringSerializer(),
                new StringSerializer()
        );
    }

    public static String getTopicIn(Config config) {
        return config.getString("kafka.topic.in");
    }

    public static String getTopicOut(Config config) {
        return config.getString("kafka.topic.out");
    }
}
