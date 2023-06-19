package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.Service;

@Slf4j
public class ServiceFiltering implements Service {
    @Override
    public void start(Config config) {
        var reader = new KafkaReaderImpl(config);
        reader.processing();
    }
}
