package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import ru.mai.lessons.rpks.KafkaReader;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;



@Slf4j
@Data
public class MyKafkaReader implements KafkaReader {
    private Rule[] rules;
    private Config config;
    private MyDbReader db;
    private ExecutorService executor;
    private String subscribeString;
    private long updateIntervalSec;
    private Properties properties;

    public MyKafkaReader(Config config) {
        this.config = config;
        properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getConfig("kafka").getConfig("consumer").getConfig("bootstrap").getString("servers"));
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test_group_consumer");
    }

    @Override
    public void processing() {
        MyKafkaWriter kafkaWriter = new MyKafkaWriter(config);
        kafkaWriter.setConfig(config);

        MyRuleProcessor processor = new MyRuleProcessor(config);
        db = new MyDbReader(config.getConfig("db"));
        rules = db.readRulesFromDB();
        updateIntervalSec = config.getConfig("application").getInt("updateIntervalSec");
        TimerTask task = new TimerTask() {
            public void run() {
                rules = db.readRulesFromDB();
                for (Rule r :
                        rules) {
                    log.info(r.toString());
                }
            }
        };

        Timer timer = new Timer(true);
        timer.schedule(task, 0, 1000 * updateIntervalSec);

        Message message;
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Collections.singleton(config.getConfig("kafka").getConfig("consumer").getString("topic")));
            ConsumerRecords<String, String> records;
            executor = Executors.newSingleThreadExecutor();
            executor.submit(() -> {

            });
            boolean exit = false;
            while (!exit) {
                records = consumer.poll(Duration.ofMillis(100));
                if (!records.isEmpty()) {
                    for (ConsumerRecord<String, String> consumerRecord : records) {
                        exit = consumerRecord.value().equals("exit");
                        message = processor.processing(new Message(consumerRecord.value(), true), rules);
                        log.info("check: " + message.getValue());
                        if (message.isDeduplicationState()) {
                            log.info("send: " + message.getValue());
                            kafkaWriter.processing(message);
                        }

                    }
                }

            }
        } catch (Exception e) {
            log.error(e.getMessage());
            Thread.currentThread().interrupt();
        }
    }
}
