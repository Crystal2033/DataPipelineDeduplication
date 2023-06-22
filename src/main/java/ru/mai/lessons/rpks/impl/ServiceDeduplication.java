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
    private Rule[] rules;
    private DbReader dbReader;


    private final Object dbLock = new Object();
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);

    private Rule[] getRules() {
        Rule[] tmpRules;
        synchronized (dbLock) {
            tmpRules = rules.clone();
        }
        return tmpRules;
    }

    private void updateRules() {
        synchronized (dbLock) {
            rules = dbReader.readRulesFromDB();
        }
    }

    @Override
    public void start(Config config) {
        RuleProcessor ruleProcessor;
        RedisClient redisClient;
        try {
            Config dbConfig = config.getConfig("db");
            Config ruleConfig = config.getConfig("application");
            Config producerConfig = config.getConfig("kafka").getConfig("producer");
            Config consumerConfig = config.getConfig("kafka").getConfig("consumer");

            dbReader = new DbReaderImpl(dbConfig);
            executor.scheduleAtFixedRate(
                    this::updateRules,
                    0,
                    Integer.parseInt(ruleConfig.getString("updateIntervalSec")),
                    TimeUnit.SECONDS
            );

            redisClient = new RedisClientImpl(config);

            ruleProcessor = new RuleProcessorImpl(redisClient);

            KafkaWriter writer = KafkaWriterImpl.builder()
                    .ruleProcessor(ruleProcessor)
                    .topic(producerConfig.getString("topic"))
                    .bootstrapServers(producerConfig.getString("bootstrap.servers"))
                    .rulesGetter(this::getRules)
                    .build();
            KafkaReader reader = KafkaReaderImpl.builder()
                    .kafkaWriter(writer)
                    .topic(consumerConfig.getString("topic"))
                    .rulesGetter(this::getRules)
                    .bootstrapServers(consumerConfig.getString("bootstrap.servers"))
                    .kafkaOffset(consumerConfig.getString("auto.offset.reset"))
                    .groupId(consumerConfig.getString("group.id"))
                    .build();
            reader.processing();
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }
}

