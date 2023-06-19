package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import ru.mai.lessons.rpks.DbReader;
import ru.mai.lessons.rpks.KafkaReader;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;

@Slf4j
public class KafkaReaderImpl implements KafkaReader {
    Rule[] rules;
    DbReader db;
    TimerTask task = new TimerTask() {
        @Override
        public void run() {
            rules = db.readRulesFromDB();
            for (var r : rules)
            {
                log.info(r.toString());
            }
        }
    };
    Config appConfig;


    public KafkaReaderImpl(Config config){
        appConfig = config;
        db = new DbReaderImpl(config);
        rules = db.readRulesFromDB();

        for (var r : rules)
        {
            log.info(r.toString());
        }


    }
    @Override
    public void processing() {
        Timer time = new Timer();
        time.schedule(task, 0, appConfig.getLong("application.updateIntervalSec") * 1000);

        Properties config = new Properties();
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put("bootstrap.servers", appConfig.getString("kafka.consumer.bootstrap.servers"));
        config.put("group.id", appConfig.getString("kafka.consumer.group.id"));
        config.put("auto.offset.reset", appConfig.getString("kafka.consumer.auto.offset.reset"));

        var ruleProcessor = new RuleProcessorImpl();
        try(KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
            var producer = new KafkaWriterImpl(appConfig);
            log.info("connect to topic {}", appConfig.getString("kafka.consumer.topic"));
            consumer.subscribe(Collections.singletonList(appConfig.getString("kafka.consumer.topic")));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (var localRecord : records) {
                    log.info("msg: {}", localRecord.value());
                    var msg = ruleProcessor.processing(new Message(localRecord.value(), false), rules);
                    if (msg.isDeduplicationState()) {
                        log.info("try to send msg: {}", msg.getValue());
                        producer.processing(msg);
                    }
                }
            }
        }
        catch (Exception e) {
            log.info("some kafka exc: {}", e.toString());
        }
    }
}
