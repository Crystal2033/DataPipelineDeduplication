package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import ru.mai.lessons.rpks.DbReader;
import ru.mai.lessons.rpks.model.Rule;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;

@Slf4j
@RequiredArgsConstructor
public class Db implements DbReader {
    private HikariDataSource hikariDataSource;
    @NonNull
    private Config config;

    @Override
    public Rule[] readRulesFromDB() {
        try (Connection connection = getConnection()) {
            DSLContext context = DSL.using(connection, SQLDialect.POSTGRES);
            String tableName = config.getConfig("db").getString("table");
            int numberOfRows = context.fetchCount(context.selectFrom(tableName));
            Rule[] ruleArray = new Rule[numberOfRows];
            ArrayList<Rule> array = new ArrayList<>();
            var rules = context.select().from(tableName).fetch();
            rules.forEach(e -> {
                Long deduplicationId = (Long) e.getValue("deduplication_id");
                Long ruleId = (Long) e.getValue("rule_id");
                String fieldName = (String) e.getValue("field_name");
                Long timeToLiveSec = (Long) e.getValue("time_to_live_sec");
                Boolean isActive = (Boolean)e.getValue("is_active");
                Rule rule = new Rule(deduplicationId, ruleId, fieldName, timeToLiveSec, isActive);
                array.add(rule);
            });
            array.toArray(ruleArray);
            return ruleArray;
        }
        catch (SQLException e) {
            log.error(e.getMessage());
            throw new IllegalStateException("DB rules error");
        }
        catch (Exception e) {
            log.error("CAUGHT FETCH EX");
            return new Rule[0];
        }
    }


    Connection getConnection() throws SQLException {
        return getDataSource().getConnection();
    }
    void closeConnection() throws SQLException {
        getDataSource().getConnection().close();
    }

    private DataSource getDataSource() {
        if (null == hikariDataSource) {
            log.info("No DataSource is available. We will create a new one.");
            createDataSource();
        }
        return hikariDataSource;
    }

    private void createDataSource() {
        try {
            HikariConfig hikariConfig = getHikariConfig();
            log.info("Configuration is ready.");
            log.info("Creating the HiKariDataSource and assigning it as the global");
            hikariDataSource = new HikariDataSource(hikariConfig);
        }
        catch (Exception e){
            log.error("caught datasource exception");
        }
    }
    private HikariConfig getHikariConfig() {
        log.info("Creating the config with HikariConfig");
        HikariConfig hikaConfig = null;
        try {
            hikaConfig = new HikariConfig();
            hikaConfig.setJdbcUrl(config.getString("db.jdbcUrl"));
            //username
            hikaConfig.setUsername(config.getString("db.user"));
            //password
            hikaConfig.setPassword(config.getString("db.password"));
            return hikaConfig;
        } catch (Exception e) {
            log.error("caught hikaconfig exception");
        }

        return hikaConfig;
    }


}
