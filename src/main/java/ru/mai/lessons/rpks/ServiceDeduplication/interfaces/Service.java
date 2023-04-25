package ru.mai.lessons.rpks.ServiceDeduplication.interfaces;

import com.typesafe.config.Config;

public interface Service {

    public void start(Config config); // стартует приложение.
}
