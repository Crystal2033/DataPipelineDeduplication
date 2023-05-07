package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import ru.mai.lessons.rpks.DbReader;
import ru.mai.lessons.rpks.model.Rule;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;

@Slf4j
@AllArgsConstructor
@NoArgsConstructor
public class MyDbReader implements DbReader {
    private HikariDataSource ds;

    public MyDbReader(Config config) {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setUsername(config.getString("user"));
        hikariConfig.setPassword(config.getString("password"));
        hikariConfig.setJdbcUrl(config.getString("jdbcUrl"));
        ds = new HikariDataSource(hikariConfig);

    }

    @Override
    public Rule[] readRulesFromDB() {
        ArrayList<Rule> result = new ArrayList<>();
        try {
            Connection connection = ds.getConnection();
            DSLContext context = DSL.using(connection, SQLDialect.POSTGRES);
            Result<Record> rules = context.select().from("public.deduplication_rules").fetch();
            rules.forEach(e -> {
                Long deduplicationId = (Long) e.getValue("deduplication_id");
                Long ruleId = (Long) e.getValue("rule_id");
                String fieldName = (String) e.getValue("field_name");
                Long timeToLiveSec = (Long) e.getValue("time_to_live_sec");
                Boolean isActive = (Boolean) e.getValue("is_active");
                Rule rule = new Rule(deduplicationId, ruleId, fieldName, timeToLiveSec, isActive);
                result.add(rule);
            });

            connection.close();
        } catch (SQLException e) {
            log.error(e.getMessage());

        }

        return result.toArray(new Rule[0]);
    }
}