package ru.mai.lessons.rpks.repository.impl;

import com.typesafe.config.Config;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.PlainSQL;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import ru.mai.lessons.rpks.model.Rule;
import ru.mai.lessons.rpks.repository.interfaces.DbReader;

import java.sql.Connection;
import java.sql.SQLException;

import static org.jooq.impl.DSL.field;

@PlainSQL
@Slf4j
@Builder
public class DataBaseReader implements DbReader, AutoCloseable {
    private final String url;
    private final String userName;
    private final String password;
    private final String driver;

    private final Config additionalDBConfig;

    private final HikariConfig config = new HikariConfig();
    private HikariDataSource dataSource;

    private DSLContext dslContext;

    private Connection dataSourceConnection;

    private void initHikariConfig() {
        config.setJdbcUrl(url);
        config.setUsername(userName);
        config.setPassword(password);
        config.setDriverClassName(driver);
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
    }

    private void initDataSourceAndDSLContext() {
        dataSource = new HikariDataSource(config);
        dslContext = DSL.using(dataSource, SQLDialect.POSTGRES);
    }


    public boolean connectToDataBase() throws SQLException {
        initHikariConfig();
        if (dataSource == null) {
            initDataSourceAndDSLContext();
        } else {
            dataSourceConnection.close();
        }
        dataSourceConnection = dataSource.getConnection();
        return dataSourceConnection.isValid(additionalDBConfig.getInt("connect_valid_time"));
    }

    public boolean isConnectedToDataBase() throws SQLException {
        return dataSourceConnection.isValid(additionalDBConfig.getInt("connect_valid_time"));
    }

    @Override
    public Rule[] readRulesFromDB() {
        return dslContext.select()
                .from(additionalDBConfig.getString("table_name"))
                .where(field(additionalDBConfig.getString("deduplication_id_column_name"))
                        .eq(additionalDBConfig.getInt(
                                additionalDBConfig.getString("deduplication_id_column_name")))
                        .and(field("is_active").eq(true)))
                .fetch()
                .stream()
                .map(note -> Rule.builder()
                        .deduplicationId((Long) note.get("deduplication_id"))
                        .ruleId((Long) note.get("rule_id"))
                        .fieldName(note.get("field_name").toString())
                        .timeToLiveSec((Long) note.get("time_to_live_sec"))
                        .isActive((Boolean) note.get("is_active"))
                        .build())
                .toList().toArray(new Rule[0]);
    }

    @Override
    public void close() throws SQLException {
        dataSourceConnection.close();
        dataSource.close();
    }
}
