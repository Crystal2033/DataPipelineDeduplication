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
import java.util.ArrayList;

import static org.jooq.impl.DSL.field;

@Slf4j
public class DbReaderImpl implements DbReader, AutoCloseable {
    private final HikariDataSource ds;

    DbReaderImpl(Config config) {
        HikariConfig configHikari = new HikariConfig();
        configHikari.setJdbcUrl(config.getString("jdbcUrl"));
        configHikari.setPassword(config.getString("password"));
        configHikari.setUsername(config.getString("user"));
        configHikari.addDataSourceProperty("cachePrepStmts", "true");
        configHikari.addDataSourceProperty("prepStmtCacheSize", "250");
        configHikari.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        ds = new HikariDataSource(configHikari);
    }

    private Connection getConnection() throws SQLException {
        return ds.getConnection();
    }

    @Override
    public Rule[] readRulesFromDB() {
        DSLContext context = null;
        ArrayList<Rule> rulesRes = new ArrayList<>();

        try (Connection connection = getConnection()){
            context = DSL.using(connection, SQLDialect.POSTGRES);
            var rules = context.select(
                    field(FIELDDBNAME.FIELDNAME.getNameField()),
                    field(FIELDDBNAME.TIMETOLIVESEC.getNameField()),
                    field(FIELDDBNAME.DEDUPLICATIONID.getNameField()),
                    field(FIELDDBNAME.ISACTIVE.getNameField()),
                    field(FIELDDBNAME.RULEID.getNameField())).
                    from("deduplication_rules").fetch();
            if (rules.isEmpty()) {
                log.info("rules is empty");
            }

            log.info("readRulesFromDB t");
            rules.forEach(ruleDb -> {
                Rule rule = new Rule();

                rule.setDeduplicationId((Long) ruleDb.getValue(FIELDDBNAME.DEDUPLICATIONID.getNameField()));
                rule.setIsActive((Boolean) ruleDb.getValue(FIELDDBNAME.ISACTIVE.getNameField()));
                rule.setFieldName((String) ruleDb.getValue(FIELDDBNAME.FIELDNAME.getNameField()));
                rule.setRuleId((Long) ruleDb.getValue(FIELDDBNAME.RULEID.getNameField()));
                rule.setTimeToLiveSec((Long) ruleDb.getValue(FIELDDBNAME.TIMETOLIVESEC.getNameField()));

                rulesRes.add(rule);
            });
            log.info(String.valueOf(rulesRes));
        } catch (SQLException e) {
            log.error("readRulesFromD:" + e.getStackTrace());
        }

        log.info("readRulesFromDB");
        return rulesRes.toArray(new Rule[0]);
    }

    @Override
    public void close() {
        ds.close();
    }

    private enum FIELDDBNAME {
        FIELDNAME("field_name"),
        TIMETOLIVESEC("time_to_live_sec"),
        ISACTIVE("is_active"),
        RULEID("rule_id"),
        DEDUPLICATIONID("deduplication_id");

        private final String nameField;

        FIELDDBNAME(String nameField) {
            this.nameField = nameField;
        }

        public String getNameField() {
            return nameField;
        }
    }
}
