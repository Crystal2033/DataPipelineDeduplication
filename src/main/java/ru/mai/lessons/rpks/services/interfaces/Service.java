package ru.mai.lessons.rpks.services.interfaces;

import com.typesafe.config.Config;

public interface Service {
    void start(Config config); // стартует приложение.
}
