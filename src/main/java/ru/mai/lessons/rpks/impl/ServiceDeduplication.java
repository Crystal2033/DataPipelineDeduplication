package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.Service;
import ru.mai.lessons.rpks.impl.settings.ConsumerSettings;
import ru.mai.lessons.rpks.impl.settings.DBSettings;
import ru.mai.lessons.rpks.impl.settings.ProducerSettings;
import ru.mai.lessons.rpks.impl.settings.RedisSettings;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class ServiceDeduplication implements Service {
    @Override
    public void start(Config config) {
        log.debug("CONFIG:"+config.toString());
        AtomicBoolean isExit=new AtomicBoolean(false);
        ConcurrentLinkedQueue<Message> concurrentLinkedQueue=new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<Rule[]> rules=new ConcurrentLinkedQueue<>();
        ExecutorService executorService= Executors.newFixedThreadPool(2);

        ConsumerSettings consumerSettings=Settings.makeConsumerSettings(config);
        ProducerSettings producerSettings=Settings.makeProducerSettings(config);
        DBSettings dbSettings=Settings.makeDBSettings(config);
        RedisSettings redisSettings=Settings.makeRedisSettings(config);
        int updateIntervalSec=config.getConfig("application").getInt("updateIntervalSec");

        ClientOfRedis clientOfRedis=ClientOfRedis.builder().redisSettings(redisSettings).build();
        ProcessorOfRule processorOfRule=ProcessorOfRule.builder().clientOfRedis(clientOfRedis).build();

        WriterToKafka writerToKafka= WriterToKafka.builder().producerSettings(producerSettings)
                .concurrentLinkedQueue(concurrentLinkedQueue).rules(rules)
                .processorOfRule(processorOfRule).build();
        ReaderFromKafka readerFromKafka= ReaderFromKafka.builder().consumerSettings(consumerSettings)
                .concurrentLinkedQueue(concurrentLinkedQueue).isExit(isExit).build();

        executorService.submit(writerToKafka::startWriter);
        executorService.submit(readerFromKafka::processing);
        ReaderFromDB readerFromDB=ReaderFromDB.builder().dbSettings(dbSettings).build();
        while(!isExit.get()) {
            rules.add(readerFromDB.readRulesFromDB());
            log.debug("ADD_RULE");
            if(rules.size()>1) {
                rules.poll();
            }
            try {
                Thread.sleep(updateIntervalSec* 1000L);
            } catch (InterruptedException e) {
                log.warn("CANT_SLEEP:"+e.getMessage());
                Thread.currentThread().interrupt();
                break;
            }
        }
        executorService.shutdown();

    }
    private  record Settings() {
        private static RedisSettings makeRedisSettings(Config config){
            Config redisConfig = config.getConfig("redis");
            RedisSettings redisSettings = RedisSettings.builder().port(redisConfig.getInt("port"))
                            .host(redisConfig.getString("host")).build();
            log.debug("REDIS_SETTINGS_WAS_READ: " + redisSettings);
            return redisSettings;
        }
        private static ConsumerSettings makeConsumerSettings(Config config) {
            Config kafkaConfigConsumer = config.getConfig("kafka").getConfig("consumer");
            ConsumerSettings consumerSettings = ConsumerSettings.builder()
                    .groupId(kafkaConfigConsumer.getString("group.id"))
                    .bootstrapServers(kafkaConfigConsumer.getString("bootstrap.servers"))
                    .autoOffsetReset(kafkaConfigConsumer.getString("auto.offset.reset"))
                    .topicIn(kafkaConfigConsumer.getString("topicIn")).build();
            log.debug("CONSUMER_SETTINGS_WAS_READ: " + consumerSettings.toString());
            return consumerSettings;
        }

        private static ProducerSettings makeProducerSettings(Config config) {
            Config kafkaConfigProducer = config.getConfig("kafka").getConfig("producer");
            ProducerSettings producerSettings = ProducerSettings.builder()
                    .bootstrapServers(kafkaConfigProducer.getString("bootstrap.servers"))
                    .topicOut(kafkaConfigProducer.getString("topicOut")).build();
            log.debug("PRODUCER_SETTINGS_WAS_READ: " + producerSettings.toString());
            return producerSettings;
        }

        private static DBSettings makeDBSettings(Config config) {
            Config dbConfig = config.getConfig("db");
            DBSettings dbSettings = DBSettings.builder().jdbcUrl(dbConfig.getString("jdbcUrl"))
                    .driver(dbConfig.getString("driver"))
                    .user(dbConfig.getString("user"))
                    .password(dbConfig.getString("password"))
                    .tableName(dbConfig.getString("tableName")).build();
            log.debug("DB_SETTINGS_WAS_READ: " + dbSettings.toString());
            return dbSettings;
        }
    }
}
