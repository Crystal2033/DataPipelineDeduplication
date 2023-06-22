package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import ru.mai.lessons.rpks.DbReader;
import ru.mai.lessons.rpks.jooq.model.Tables;
import ru.mai.lessons.rpks.model.Rule;

import java.sql.Connection;
import java.sql.SQLException;

@Slf4j
public class DbReaderImpl implements DbReader {
    private final HikariDataSource hikari;
    private final DSLContext dslContext;

    public DbReaderImpl(Config dbConfig) {
        HikariConfig hikariDbConfig = new HikariConfig();
        hikariDbConfig.setJdbcUrl(dbConfig.getString("jdbcUrl"));
        hikariDbConfig.setDriverClassName(dbConfig.getString("driver"));
        hikariDbConfig.setUsername(dbConfig.getString("user"));
        hikariDbConfig.setPassword(dbConfig.getString("password"));

        hikari = new HikariDataSource(hikariDbConfig);
        dslContext = DSL.using(hikari, SQLDialect.POSTGRES);
    }

    @Override
    public Rule[] readRulesFromDB() {
        log.debug("Start reading rules from the database");
        try (Connection connection = hikari.getConnection()) {
            return dslContext.select().from(Tables.DEDUPLICATION_RULES).fetchInto(Rule.class).toArray(Rule[]::new);
        } catch (SQLException e) {
            log.error("An error occurred while reading rules from the database: {}", e.getMessage());
        }
        return new Rule[0];
    }
}