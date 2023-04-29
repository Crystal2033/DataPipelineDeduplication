package ru.mai.lessons.rpks.services.interfaces;

import com.typesafe.config.Config;

public interface Service {
    public void start(Config config); // стартует приложение.
}
