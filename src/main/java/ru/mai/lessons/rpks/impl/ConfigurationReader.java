package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import ru.mai.lessons.rpks.ConfigReader;

import static ru.mai.lessons.rpks.impl.constants.MainNames.CONF_PATH;

public class ConfigurationReader implements ConfigReader {
    @Override
    public Config loadConfig() {
        return ConfigFactory.load(CONF_PATH);
    }
}
