package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import ru.mai.lessons.rpks.ConfigReader;

public final class ConfigReaderImpl implements ConfigReader {
    @Override
    public Config loadConfig() {
        return ConfigFactory.load();
    }
}
