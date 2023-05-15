package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.Service;


@Slf4j
public class ServiceDeduplication implements Service {

    @Override
    public void start(Config config) {
        // написать код реализации сервиса дедупликации
        MyKafkaReader kafkaReader = new MyKafkaReader(config);
        kafkaReader.processing();
        log.info("exit from ServiceDeduplication.start");
    }
}
