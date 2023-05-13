package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.DbReader;
import ru.mai.lessons.rpks.KafkaReader;
import ru.mai.lessons.rpks.Service;
import ru.mai.lessons.rpks.model.Rule;

import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class ServiceDeduplication implements Service {
    @Override
    public void start(Config config) {
        // написать код реализации сервиса фильтрации
        // Считываем параметры конфигурации для подключения к базе данных
        Config db = config.getConfig("db");
        String url = db.getString("jdbcUrl");
        String user = db.getString("user");
        String password = db.getString("password");
        String driver = db.getString("driver");
        Long updateIntervalSec = config.getLong("application.updateIntervalSec");
        DbReader dbreader = new ReaderDB(url, user, password, driver);

        Queue<Rule[]> queue = new ConcurrentLinkedQueue<>();
        new Thread(() -> {
            try {
                String fieldName = "";
                while(!Objects.equals(fieldName, "exit")) {
                    //Считываем правила из БД
                    Rule[] rules = dbreader.readRulesFromDB();
                    if(!queue.isEmpty()) queue.remove();
                    queue.add(rules);

                    fieldName = rules[0].getFieldName();
                    Thread.sleep(updateIntervalSec * 1000);
                }
            } catch (InterruptedException ex) {
                log.warn("Interrupted!", ex);
                Thread.currentThread().interrupt();
            }
        }).start();

        //Kafka reader
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        executorService.execute(() -> {
            KafkaReader kafkaReader = new KafReader(config, queue);
            kafkaReader.processing();
        });
        executorService.shutdown(); // Завершить после выполнения все задач.
    }
}
