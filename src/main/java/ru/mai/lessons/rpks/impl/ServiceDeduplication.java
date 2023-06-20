package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import ru.mai.lessons.rpks.KafkaReader;
import ru.mai.lessons.rpks.Service;
import ru.mai.lessons.rpks.model.Rule;

import java.util.ArrayList;
import java.util.List;

public class ServiceDeduplication implements Service {

    List<Rule> rules = new ArrayList<>();
    private Config config;
    private KafkaReaderRealization kafkaReader = new KafkaReaderRealization();
    @Override
    public void start(Config config) {
        // написать код реализации сервиса фильтрации
        kafkaReader.setConfig(config);
        kafkaReader.setRuleList(rules);
        kafkaReader.processing();
    }
}
