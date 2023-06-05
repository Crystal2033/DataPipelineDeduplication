package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import ru.mai.lessons.rpks.KafkaReader;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.time.Duration;
import java.util.*;

@Slf4j
@Getter
@Setter
@RequiredArgsConstructor
public class KafkaReaderImpl implements KafkaReader {

    private final String topic;
    private final String topicOut;
    private final String bootstrapServers;
    private final String bootstrapServersWriter;
    @NonNull
    Rule[] rules;
    @NonNull
    Config config;
    private boolean isExit = false;


    public void processing() {
        KafkaWriterImpl kafkaWriter = new KafkaWriterImpl(config);
        RuleProcessorImpl ruleProcessor = new RuleProcessorImpl(config);
        int updateIntervalSec = config.getInt("application.updateIntervalSec");
        Db db = new Db(config);
        rules = db.readRulesFromDB();
        log.info("Start reading kafka topic {}", topic);
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                        ConsumerConfig.GROUP_ID_CONFIG, "tc-" + UUID.randomUUID(),
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, config.getString("kafka.consumer.auto.offset.reset")
                ),
                new StringDeserializer(),
                new StringDeserializer()
        );
        TimerTask task = new TimerTask() {
            public void run() {
                rules = db.readRulesFromDB();
            }
        };

        Timer timer = new Timer(true);

        timer.schedule(task, 0, 1000L * updateIntervalSec);


        kafkaConsumer.subscribe(Collections.singletonList(topic));
        try (kafkaConsumer) {
            while (!isExit) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords)
                {
                    Message msg = new Message(consumerRecord.value(), true);
                    Message processedMsg = ruleProcessor.processing(msg, rules);
                    if (processedMsg.isDeduplicationState()) {
                        log.debug("Message state{} {}", processedMsg.getValue(), processedMsg.isDeduplicationState());
                        kafkaWriter.processing(processedMsg);
                    }
                }
            }
            log.info("Read is done!");
        }
    }

}