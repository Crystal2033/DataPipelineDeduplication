package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.*;
import ru.mai.lessons.rpks.model.Rule;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ServiceDeduplication implements Service {
    private static final String RULE_CONFIG = "application";
    private static final String DB_CONFIG = "db";
    private static final String KAFKA_CONFIG = "kafka";
    private static final String REDIS_CONFIG = "redis";

    private Rule[] rules;
    private DbReader dbReader;
    private final Object dbLock = new Object();
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);

    @Override
    public void start(Config config) {
        dbReader = new DbReaderImpl(config.getConfig(DB_CONFIG));

        executor.scheduleAtFixedRate(
                this::updateRules,
                0,
                Integer.parseInt(config.getConfig(RULE_CONFIG).getString("updateIntervalSec")),
                TimeUnit.SECONDS
        );

        Config producer = config.getConfig(KAFKA_CONFIG).getConfig("producer");
        KafkaWriter writer = KafkaWriterImpl.builder()
                .ruleProcessor(new RuleProcessorImpl(new RedisClientImpl(config.getConfig(REDIS_CONFIG))))
                .rulesGetter(this::getRules)
                .topic(producer.getString("topic"))
                .bootstrapServers(producer.getString("bootstrap.servers"))
                .build();

        Config consumer = config.getConfig(KAFKA_CONFIG).getConfig("consumer");
        KafkaReader reader = KafkaReaderImpl.builder()
                .kafkaWriter(writer)
                .topic(consumer.getString("topic"))
                .bootstrapServers(consumer.getString("bootstrap.servers"))
                .kafkaOffset(consumer.getString("auto.offset.reset"))
                .groupId(consumer.getString("group.id"))
                .build();

        reader.processing();
    }

    private Rule[] getRules() {
        synchronized (dbLock) {
            if (rules == null) {
                try {
                    rules = dbReader.readRulesFromDB();
                }  catch (IllegalStateException ignored) {
                    rules = null;
                }
            }
            return rules;
        }
    }

    private void updateRules() {
        log.info("DB checking is ON");
        synchronized (dbLock) {
            try {
                rules = dbReader.readRulesFromDB();
            }  catch (IllegalStateException ignored) {
                rules = null;
            }
        }
        log.info("DB checking is OFF");
    }
}
