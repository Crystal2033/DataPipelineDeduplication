package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.Service;
import ru.mai.lessons.rpks.model.Rule;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
@Slf4j
public class ServiceDeduplication implements Service {
    Rule[] rules;
    @Override
    public void start(Config config) {
        rules = new Rule[1];
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        String reader = config.getString("kafka.consumer.bootstrap.servers");
        String writer = config.getString("kafka.producer.bootstrap.servers");
        KafkaReaderImpl kafkaReader = new KafkaReaderImpl("test_topic_in", "test_topic_out", reader, writer, rules, config);
        executorService.execute(kafkaReader::processing);

        // написать код реализации сервиса фильтрации
    }
}
