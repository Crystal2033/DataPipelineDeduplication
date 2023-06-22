package ru.mai.lessons.rpks;

import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.impl.ConfigurationReader;
import ru.mai.lessons.rpks.impl.ServiceDeduplication;

@Slf4j
public class ServiceDeduplicationMain {
    public static void main(String[] args) {
        log.info("Start service Deduplication");
        ConfigReader configReader = new ConfigurationReader();
        Service service = new ServiceDeduplication(); // ваша реализация service
        log.info("Start service Deduplication");
        service.start(configReader.loadConfig());
    }
}