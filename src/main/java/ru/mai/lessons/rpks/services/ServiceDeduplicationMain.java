package ru.mai.lessons.rpks.services;

import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.configs.ConfigurationReader;
import ru.mai.lessons.rpks.configs.interfaces.ConfigReader;
import ru.mai.lessons.rpks.services.interfaces.Service;

@Slf4j
public class ServiceDeduplicationMain {
    public static void main(String[] args) {
        log.info("Start service Deduplication");
        ConfigReader configReader = new ConfigurationReader();
        Service service = new ServiceDeduplication(); // ваша реализация service
        service.start(configReader.loadConfig());
        log.info("Terminate service Deduplication");
    }
}