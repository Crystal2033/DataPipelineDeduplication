package ru.mai.lessons.rpks.config;

import com.typesafe.config.Config;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.Getter;

@Getter
public final class DbConfig {

    public static HikariDataSource createConnectionPool(Config conf) {
        var config = new HikariConfig();
        config.setJdbcUrl(conf.getString("db.jdbcUrl"));
        config.setUsername(conf.getString("db.user"));
        config.setPassword(conf.getString("db.password"));
        config.setDriverClassName(conf.getString("db.driver"));
        return new HikariDataSource(config);
    }
}
