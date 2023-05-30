package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.Service;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
@Slf4j
public class ServiceDeduplication implements Service {
    Db db;
    Rule[] rules;
    int updateIntervalSec;
//     ConcurrentLinkedQueue<Message> queue;

    @Override
    public void start(Config config) {
        rules = new Rule[1];
        ExecutorService executorService = Executors.newFixedThreadPool(1);
//        KafkaReaderImpl kafkaReader = new KafkaReaderImpl(config);
        updateIntervalSec = config.getInt("application.updateIntervalSec");
//        queue = new ConcurrentLinkedQueue<>();
        db = new Db(config);
        rules = db.readRulesFromDB();
//        KafkaReaderImpl kafkaReader = new KafkaReaderImpl( rules, config);
        String reader = config.getString("kafka.consumer.bootstrap.servers");
        String writer = config.getString("kafka.producer.bootstrap.servers");
        KafkaReaderImpl kafkaReader = new KafkaReaderImpl("test_topic_in", "test_topic_out", reader, writer, rules, config);

        TimerTask task = new TimerTask() {
            public void run() {
                rules = db.readRulesFromDB();
                for (Rule r :
                        rules) {
                    log.info(r.toString());
                    log.info("TIMER");

                }
                kafkaReader.setRules(rules);
            }
        };

        Timer timer = new Timer(true);

        timer.schedule(task, 0, 1000L * updateIntervalSec);
        log.info("delay:" + updateIntervalSec);



//        executorService.execute(() -> {
//            queue = kafkaReader.getQueue();
//            log.info("+++++++" + queue);
//        });


        executorService.execute(() -> {
            kafkaReader.setRules(rules);
            log.info("NEW RULES SET {}", rules.length);
            kafkaReader.processing();
        });

//        Runtime.getRuntime().addShutdownHook(new Thread() {
//            @Override
//            public void run() {
//                task.cancel();
//                timer.cancel();
//            }
//        });

        // написать код реализации сервиса фильтрации
    }
}
