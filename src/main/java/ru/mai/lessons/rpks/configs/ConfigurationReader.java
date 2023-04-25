package ru.mai.lessons.rpks.configs;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import ru.mai.lessons.rpks.configs.interfaces.ConfigReader;

import static ru.mai.lessons.rpks.constants.MainNames.CONF_PATH;

public class ConfigurationReader implements ConfigReader {
    @Override
    public Config loadConfig() {
        return ConfigFactory.load(CONF_PATH);
    }
}
