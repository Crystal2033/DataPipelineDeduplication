package ru.mai.lessons.rpks.ServiceDeduplication;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.ServiceDeduplication.interfaces.Service;
import ru.mai.lessons.rpks.kafka.impl.KafkaReaderImpl;
import ru.mai.lessons.rpks.kafka.interfaces.DispatcherKafka;
import ru.mai.lessons.rpks.model.Rule;
import ru.mai.lessons.rpks.repository.impl.DataBaseReader;
import ru.mai.lessons.rpks.repository.impl.RulesUpdaterThread;

import java.io.Closeable;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static ru.mai.lessons.rpks.constants.MainNames.KAFKA_NAME;
import static ru.mai.lessons.rpks.constants.MainNames.TOPIC_NAME_PATH;

@Slf4j
public class ServiceDeduplication implements Service {
    private final ConcurrentHashMap<String, List<Rule>> rulesConcurrentMap = new ConcurrentHashMap<>();
    private Config outerConfig;
    @Override
    public void start(Config config) {
        outerConfig = config;
        try (DataBaseReader dataBaseReader = initExistingDBReader(outerConfig.getConfig("db"))) {
            connectToDBAndWork(dataBaseReader);
        } catch (SQLException e) {
            log.error("There is a problem with initializing database.");
        }
    }

    private void startKafkaReader(DispatcherKafka dispatcherKafka) {

        Config config = outerConfig.getConfig(KAFKA_NAME).getConfig("consumer");

        KafkaReaderImpl kafkaReader = KafkaReaderImpl.builder()
                .topic(config.getConfig("deduplication").getString(TOPIC_NAME_PATH))
                .autoOffsetReset(config.getString(("auto.offset.reset")))
                .bootstrapServers(config.getString("bootstrap.servers"))
                .groupId(config.getString("group.id"))
                .dispatcherKafka(dispatcherKafka)
                .exitWord(outerConfig.getConfig(KAFKA_NAME).getString("exit.string"))
                .build();

        kafkaReader.processing();
    }

    private DataBaseReader initExistingDBReader(Config configDB) {
        return DataBaseReader.builder()
                .url(configDB.getString("jdbcUrl"))
                .userName(configDB.getString("user"))
                .password(configDB.getString("password"))
                .driver(configDB.getString("driver"))
                .additionalDBConfig(configDB.getConfig("additional_info"))
                .build();
    }

    private void connectToDBAndWork(DataBaseReader dataBaseReader) {
        ExecutorService executorService = Executors.newCachedThreadPool();
        try (Closeable service = executorService::shutdownNow) {
            connectAndRun(dataBaseReader, executorService);
        } catch (IOException e) {
            log.error("There is an error with binding Closeable objects");
        }

    }

    private void connectAndRun(DataBaseReader dataBaseReader, ExecutorService executorService) {
        try {
            if (dataBaseReader.connectToDataBase()) {

                RulesUpdaterThread rulesDBUpdaterThread = new RulesUpdaterThread(rulesConcurrentMap, dataBaseReader, outerConfig);

//                Config config = outerConfig.getConfig(KAFKA_NAME)
//                        .getConfig("producer");
//                DispatcherKafka filterDispatcher = new DeduplicationDispatcher(config.getConfig("deduplication")
//                        .getString(TOPIC_NAME_PATH), config.getString("bootstrap.servers"), rulesDBUpdaterThread);
//
//
                executorService.execute(rulesDBUpdaterThread);
//
                startKafkaReader(null);
                executorService.shutdown();
            } else {
                log.error("There is a problem with connection to database.");
            }
        } catch (SQLException exc) {
            log.error("There is a problem with getConnection from Hikari.");
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        } finally {
            log.info("All threads are done.");
            executorService.shutdownNow();
        }
    }
}
