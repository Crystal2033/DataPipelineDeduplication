package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import ru.mai.lessons.rpks.DbReader;
import ru.mai.lessons.rpks.model.Rule;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Objects;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

@Slf4j
@RequiredArgsConstructor
@Setter
public class DbReaderImpl implements DbReader {

    private DataSource dataSource;

    @NonNull
    Config config;

    private final String deduplicationId = "deduplication_id";
    private final String ruleId = "rule_id";
    private final String fieldName = "field_name";
    private final String timeToLiveSec = "time_to_live_sec";
    private final String isActive = "is_active";

    private Connection getConnection() throws SQLException {
        return getDataSource().getConnection();
    }

    private DataSource getDataSource() {
        if (dataSource == null) {
            createDataSource();
        }
        log.info("Get datasource successfully");
        return dataSource;
    }

    void createDataSource() {
        try {
            String driver = config.getString("db.driver");
            Class.forName(driver);
            HikariConfig hikariConfigConfig = createHikariConfig();
            this.dataSource = new HikariDataSource(hikariConfigConfig);
            log.info("Created a new datasource");
        } catch (ClassNotFoundException e) {
            log.error("Class not found exception (create data source)");
        }
    }

    HikariConfig createHikariConfig() {
        String driver = config.getString("db.driver");
        HikariConfig hikariConfig = new HikariConfig();
        try {
            Class.forName(driver);
            hikariConfig.setJdbcUrl(config.getString("db.jdbcUrl"));
            hikariConfig.setUsername(config.getString("db.user"));
            hikariConfig.setPassword(config.getString("db.password"));
            hikariConfig.setDriverClassName(driver);
            log.info("Hikari config was done");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return hikariConfig;
    }

    @Override
    public Rule[] readRulesFromDB() {

        try (Connection connection = getConnection()) {
            DSLContext dsl = DSL.using(connection, SQLDialect.POSTGRES);
            String tableName = config.getString("db.table");

            ArrayList<Rule> rulesFromDb = new ArrayList<>();

            var selectFromDb = dsl.select(
                    field(deduplicationId),
                    field(ruleId),
                    field(fieldName),
                    field(timeToLiveSec),
                    field(isActive)
            ).from(table(tableName)).fetch();

            selectFromDb.forEach(row -> {
                Long fieldDeduplicationId = (Long) row.getValue(field(deduplicationId));
                Long fieldRuleId = (Long) row.getValue(field(ruleId));
                String fieldFieldName = row.getValue(field(fieldName)).toString();
                Long fieldTimeToLiveSec = (Long) row.getValue(field(timeToLiveSec));
                boolean fieldIsActive = (boolean) row.getValue(field(isActive));
                if (fieldIsActive) {
                    Rule rule = new Rule(fieldDeduplicationId, fieldRuleId, fieldFieldName, fieldTimeToLiveSec, true);
                    rulesFromDb.add(rule);
                }
            });
            Rule[] rules = new Rule[rulesFromDb.size()];
            rulesFromDb.toArray(rules);
            return rules;
        } catch (SQLException e) {
            log.error("Connection was failed!");
            throw new IllegalStateException("DB is not ready");
        }
        catch (Exception e) {
            e.printStackTrace();
            return new Rule[0];
        }
    }
}
