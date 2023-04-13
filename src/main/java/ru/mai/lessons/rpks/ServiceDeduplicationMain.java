package ru.mai.lessons.rpks;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jooq.codegen.GenerationTool;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.JedisPubSub;
import ru.mai.lessons.rpks.impl.ConfigReaderImpl;
import ru.mai.lessons.rpks.impl.ServiceDeduplication;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

@Slf4j
class Audit extends JedisPubSub {

    @Override
    public void onSubscribe(String channel, int subscribedChannels) {
        log.info("Subscribe to channel name: {}", channel);
    }

    @Override
    public void onUnsubscribe(String channel, int subscribedChannels) {
        log.info("Unsubscribe to channel name: {}", channel);
    }

    @Override
    public void onMessage(String channel, String message) {
        log.info("Get message {} from channel {}", message, channel);
        if (message.contains("exit")) {
            unsubscribe();
        }
    }
}

@Slf4j
@Data
@RequiredArgsConstructor
class RedisClient1 {
    private final String host;
    private final int port;

    private JedisPooled jedis;

    private boolean enabledAudit = false;

    private static final String AUDIT_CHANNEL = "audit";

    public void writeData(String key, String value) {
        log.info("Save data to Redis: {} = {}", key, value);
        getJedis().set(key, value);
        if (enabledAudit) {
            getJedis().publish(AUDIT_CHANNEL, "set key " + key + ", value " + value);
        }
    }

    public String readData(String key) {
        log.info("Read data from Redis by key: {}", key);
        if (enabledAudit) {
            getJedis().publish(AUDIT_CHANNEL, "get " + key);
        }
        return getJedis().get(key);
    }

    public void expire(String key, long seconds) {
        log.info("Set time to live {} seconds by key {}", seconds, key);
        getJedis().expire(key, seconds);
        if (enabledAudit) {
            getJedis().publish(AUDIT_CHANNEL, "expire " + key + " " + seconds + " seconds");
        }
    }

    public void audit() {
        log.info("Enable audit. Create pub/sub channel '{}'", AUDIT_CHANNEL);
        getJedis().subscribe(new Audit(), AUDIT_CHANNEL);
        enabledAudit = true;
    }

    private JedisPooled getJedis() {
        return Optional.ofNullable(jedis).orElse(new JedisPooled(host, port));
    }
}

@Slf4j
public class ServiceDeduplicationMain {


    public static void main(String[] args) throws Exception {
        GenerationTool.generate(
                Files.readString(
                        Path.of("jooq-config.xml")
                )
        );

//        log.info("Start service Deduplication");
//        ConfigReader configReader = new ConfigReaderImpl();
//        Service service = new ServiceDeduplication(); // ваша реализация service
//        service.start(configReader.loadConfig());
//        log.info("Terminate service Deduplication");

//        log.info("Start application");
//        RedisClient1 redisClient = new RedisClient1("localhost", 6379);
//        redisClient.writeData("testKey", "testValue");
//        String dataFromRedis = redisClient.readData("testKey");
//        log.info("Data from Redis: {}", dataFromRedis);
//        redisClient.expire("testKey", 2);
//        try {
//            log.info("Wait...");
//            Thread.sleep(3000);
//        } catch (InterruptedException e) {
//            log.error("Interrupt thread", e);
//        }
//
//        log.info("Data from Redis: {}", redisClient.readData("testKey"));
//
//        redisClient.setEnabledAudit(true);
//
//        redisClient.writeData("testPublishKey", "testPublishValue");
//        redisClient.expire("testPublishKey", 3);
//        log.info("Data from Redis: {}", redisClient.readData("testPublishKey"));
//        try {
//            log.info("Wait...");
//            Thread.sleep(4000);
//        } catch (InterruptedException e) {
//            log.error("Interrupt thread", e);
//        }
//        log.info("Data from Redis: {}", redisClient.readData("testPublishKey"));
//
//        try (Scanner scanner = new Scanner(System.in)) {
//            String inputData = "";
//            do {
//                log.info("Enter input data:");
//                inputData = scanner.nextLine();
//                if (inputData.contains(":")) {
//                    String[] keyValue = inputData.split(":");
//                    redisClient.writeData(keyValue[0], keyValue[1]);
//                } else if (inputData.equalsIgnoreCase("exit")) {
//                    redisClient.writeData("", inputData);
//                }
//            } while (!inputData.equalsIgnoreCase("exit"));
//        }
//
//        log.info("End application");
    }
}