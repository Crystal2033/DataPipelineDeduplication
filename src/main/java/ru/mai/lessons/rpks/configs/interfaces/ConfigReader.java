package ru.mai.lessons.rpks.configs.interfaces;

import com.typesafe.config.Config;

public interface ConfigReader {
    Config loadConfig(); // метод читает конфигурацию из файла *.conf
}
