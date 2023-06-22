package ru.mai.lessons.rpks.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import ru.mai.lessons.rpks.KafkaReader;
import ru.mai.lessons.rpks.model.Message;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

@Slf4j
@RequiredArgsConstructor
public class KafkaReaderImpl implements KafkaReader {
    private final String topic;
    private final String bootstrapServers;
    private final String groupId;
    private final String autoOffsetReset;
    private final MessageHandler messageHandler;

    @Override
    public void processing() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);

        log.info("topic " + topic);

        try(KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(List.of(topic));

            while (true) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> recordMsg : records) {
                    messageHandler.processMessage(Message.builder().value(recordMsg.value()).deduplicationState(false).build());
                }
            }

        } catch (WakeupException e) {
            log.error("Wake up exception!");
        } catch (Exception e) {
            log.error("Unexpected exception", e);
        } finally {
            log.info("The consumer is now gracefully closed.");
        }

    }
}
