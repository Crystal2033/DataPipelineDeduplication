package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.DbReader;
import ru.mai.lessons.rpks.Service;
import ru.mai.lessons.rpks.model.Rule;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
@Slf4j
public class ServiceDeduplication implements Service {
    DbReader db;
    Rule[] rules;
    @Override
    public void start(Config config) {
        // написать код реализации сервиса фильтрации
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        int updateIntervalSec = config.getInt("application.updateIntervalSec");

        db = new DbReaderImpl(config);
        rules = db.readRulesFromDB();
        String consumer = config.getString("kafka.consumer.bootstrap.servers");
        String topicIn = config.getString("kafka.topic_in");

        KafkaReaderImpl kafkaReader = new KafkaReaderImpl(consumer, topicIn, config, rules);

        TimerTask task = new TimerTask() {
            public void run() {
                rules = db.readRulesFromDB();
                for (Rule r : rules) {
                    log.info(r.toString());
                }
                kafkaReader.setRules(rules);
            }
        };

        Timer timer = new Timer(true);

        timer.schedule(task, 0, 1000L * updateIntervalSec);

        executorService.execute(() -> {
            kafkaReader.setRules(rules);
            kafkaReader.processing();
        });

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                task.cancel();
                timer.cancel();
            }
        });
    }
}
