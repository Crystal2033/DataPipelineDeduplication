package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.serialization.StringDeserializer;
import ru.mai.lessons.rpks.KafkaReader;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;


@Slf4j
@Data
public class MyKafkaReader implements KafkaReader {
    private Rule[] rules;
    private Config config;
    private MyDbReader db;
    private String subscribeString;
    private long updateIntervalSec;
    private Properties properties;
    private KafkaConsumer<String, String> consumer;


    public MyKafkaReader(Config config) {
        this.config = config;
        Config configKafkaConsumer = config.getConfig("kafka").getConfig("consumer");
        properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, configKafkaConsumer.getConfig("bootstrap").getString("servers"));
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, configKafkaConsumer.getConfig("auto").getConfig("offset").getString("reset"));
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, configKafkaConsumer.getConfig("group").getString("id"));
        consumer = new KafkaConsumer<>(properties);

    }

    @Override
    public void processing() {
        MyKafkaWriter kafkaWriter = new MyKafkaWriter(config);
        MyRuleProcessor processor = new MyRuleProcessor(config);
        db = new MyDbReader(config.getConfig("db"));
        rules = db.readRulesFromDB();
        updateIntervalSec = config.getConfig("application").getInt("updateIntervalSec");
        TimerTask task = new TimerTask() {
            public void run() {
                rules = db.readRulesFromDB();
                for (Rule r :
                        rules) {
                    log.debug(r.toString());
                }
            }
        };
        Timer timer = new Timer(true);
        timer.schedule(task, 0, 1000 * updateIntervalSec);

        try {
            consumer.subscribe(Collections.singleton(config.getConfig("kafka").getConfig("consumer").getString("topic")));
            ConsumerRecords<String, String> records;
            Message message;
            while (!Thread.interrupted()) {
                records = consumer.poll(Duration.ofMillis(100));
                if (!records.isEmpty()) {
                    for (ConsumerRecord<String, String> consumerRecord : records) {
                        message = processor.processing(new Message(consumerRecord.value(), true), rules);
                        log.debug("check: " + message.getValue());
                        if (message.isDeduplicationState()) {
                            kafkaWriter.processing(message);
                        }

                    }
                }

            }
        } catch (InterruptException e) {
            log.info("exit from kafka reader");
        }
    }
}
