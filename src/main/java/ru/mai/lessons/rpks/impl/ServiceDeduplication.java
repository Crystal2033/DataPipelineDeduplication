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
    private static final String DATA_BASE_CONFIG_NAME = "db";
    private static final String KAFKA_CONFIG_NAME = "kafka";
    private static final String RULE_INTERVAL_CONFIG_NAME = "application";
    private static final String REDIS_CONFIG_NAME = "redis";

    private Rule[] rules;
    private DbReader dbReader;
    private RuleProcessor ruleProcessor;

    private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
    private final Object lock = new Object();

    @Override
    public void start(Config config) {
        RedisClient redisClient = new RedisClientImpl(config.getConfig(REDIS_CONFIG_NAME));
        ruleProcessor = new RuleProcessorImpl(redisClient);
        dbReader = new DbReaderImpl(config.getConfig(DATA_BASE_CONFIG_NAME));

        initScheduledExecutorServiceForRuleUpdate(config.getConfig(RULE_INTERVAL_CONFIG_NAME));
        startKafka(config.getConfig(KAFKA_CONFIG_NAME));
    }

    private void initScheduledExecutorServiceForRuleUpdate(Config ruleIntervalConfig) {
        String interval = ruleIntervalConfig.getString("updateIntervalSec");

        executorService.scheduleAtFixedRate(
                this::updateRules,
                0,
                Integer.parseInt(interval),
                TimeUnit.SECONDS
        );
    }

    private void startKafka(Config kafkaConfig) {
        Config producerConfig = kafkaConfig.getConfig("producer");
        Config consumerConfig = kafkaConfig.getConfig("consumer");

        KafkaWriter kafkaWriter = KafkaWriterImpl.builder()
                .ruleProcessor(ruleProcessor)
                .rulesGetter(this::getRules)
                .topic(producerConfig.getString("topic"))
                .bootstrapServers(producerConfig.getString("bootstrap.servers"))
                .build();

        KafkaReader kafkaReader = KafkaReaderImpl.builder()
                .kafkaWriter(kafkaWriter)
                .topic(consumerConfig.getString("topic"))
                .groupId(consumerConfig.getString("group.id"))
                .kafkaOffset(consumerConfig.getString("auto.offset.reset"))
                .bootstrapServers(consumerConfig.getString("bootstrap.servers"))
                .build();

        kafkaReader.processing();
    }

    private void updateRules() {
        log.debug("Start reading rules from DB");
        synchronized (lock) {
            rules = dbReader.readRulesFromDB();
        }
        log.debug("End reading rules from DB");
    }

    private Rule[] getRules() {
        synchronized (lock) {
            if (rules == null) {
                rules = dbReader.readRulesFromDB();
            }
            return rules;
        }
    }
}
