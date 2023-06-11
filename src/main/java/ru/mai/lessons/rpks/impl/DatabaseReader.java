package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import ru.mai.lessons.rpks.DbReader;
import ru.mai.lessons.rpks.model.Rule;

import java.sql.Connection;
import java.sql.SQLException;

@Slf4j
public class DatabaseReader implements DbReader {
    private HikariDataSource dataSource;
    private final HikariConfig hikariConfig = new HikariConfig();

    DatabaseReader(Config config) {
        setConfig(config);
    }
    public void setConfig(Config config) {
        hikariConfig.setJdbcUrl(config.getString("jdbcUrl"));
        hikariConfig.setUsername(config.getString("user"));
        hikariConfig.setPassword(config.getString("password"));
        hikariConfig.setDriverClassName(config.getString("driver"));
        dataSource = new HikariDataSource(hikariConfig);
    }
    @Override
    public Rule[] readRulesFromDB() {
        try (Connection connection = dataSource.getConnection()) {
            DSLContext context = DSL.using(connection, SQLDialect.POSTGRES);
            return context.select().from("deduplication_rules").fetch().into(Rule.class).toArray(Rule[]::new);
        }
        catch (SQLException e) {
            log.error("Something went wrong with DB!");
        }
        return new Rule[0];
    }
}
