package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.*;
import ru.mai.lessons.rpks.model.Rule;

import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ServiceDeduplication implements Service {
    private Rule[] rules;
    private DbReader dbReader;
    private final Object locker = new Object();
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);

    private void loadRules() {
        synchronized (locker) {
            rules = Arrays.stream(dbReader.readRulesFromDB()).filter(rule -> !rule.getFieldName().isBlank()).toArray(Rule[]::new);
        }

    }

    private Rule[] getRules() {
        Rule[] resRules;
        synchronized (locker) {
            resRules = rules.clone();
        }
        return resRules;
    }

    @Override
    public void start(Config config) {

        dbReader = new DbReaderImpl(config.getConfig("db"));
        executor.scheduleAtFixedRate(
                this::loadRules,
                0,
                Integer.parseInt(config.getConfig("application").getString("updateIntervalSec")),
                TimeUnit.SECONDS
        );

        RedisClient redisClient = new RedisClientImpl(config.getConfig("redis"));

        RuleProcessor ruleProcessor = new RuleProcessorImpl(redisClient);

        Config kafka = config.getConfig("kafka");
        Config producer = kafka.getConfig("producer");

        KafkaWriter kafkaWriter = new KafkaWriterImpl(producer.getString("topic"),
                producer.getString("bootstrap.servers"));

        Config consumer = kafka.getConfig("consumer");

        KafkaReader kafkaReader = new KafkaReaderImpl(kafkaWriter, this::getRules, ruleProcessor, consumer.getString("topic"),
                consumer.getString("auto.offset.reset"),
                consumer.getString("group.id"),
                consumer.getString("bootstrap.servers"));




        kafkaReader.processing();

    }
}
