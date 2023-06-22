package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.Package;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import ru.mai.lessons.rpks.DbReader;
import ru.mai.lessons.rpks.model.Rule;

import java.sql.Connection;
import java.sql.SQLException;

@Slf4j
public class DataBaseReader implements DbReader {

    private HikariDataSource hikariDataSource;

    public DataBaseReader(Config config) {
        // Hikari
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(config.getString("db.jdbcUrl"));
        hikariConfig.setUsername(config.getString("db.user"));
        hikariConfig.setPassword(config.getString("db.password"));
        hikariConfig.setDriverClassName(config.getString("db.driver"));

        log.debug("Init HikariDataSource");
        hikariDataSource = new HikariDataSource(hikariConfig);
    }

    // JOOQ to make requests
    private DSLContext dslContext;

    @Override
    public Rule[] readRulesFromDB() {
        return dslContext.select().from("public.deduplication_rules").fetchInto(Rule.class).toArray(Rule[]::new);
    }
    public boolean connectToDataBase() throws SQLException {
        log.info("Init DSL");
        dslContext = DSL.using(hikariDataSource, SQLDialect.POSTGRES);

        return isConnected();
    }

    public Connection getConnection() throws SQLException {
        if (hikariDataSource != null)
            return hikariDataSource.getConnection();
        else
            return null;
    }

    public boolean isConnected() throws SQLException {
        if (getConnection() == null) {
            return false;
        }
        else {
            return getConnection().isValid(0);
        }
    }

}
