package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import ru.mai.lessons.rpks.KafkaReader;
import ru.mai.lessons.rpks.KafkaWriter;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;
@Slf4j
public class KafkaReaderI implements KafkaReader {
    private final KafkaConsumer<String, String> consumer;
    private final KafkaWriter writer;
    private final RuleProcessorI checkerRules;
    private final Supplier<Rule[]> supplier;

    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private Rule [] rules;
    public KafkaReaderI(Config conf, KafkaWriter writer, RuleProcessorI checker, Supplier<Rule[]> supplier){
        this.writer = writer;
        this.supplier = supplier;
        Properties props = new Properties();
        Config consumerConfig = conf.getConfig("consumer");
        props.put("bootstrap.servers", consumerConfig.getString("bootstrap.servers"));
        props.put("group.id", consumerConfig.getString("group.id"));
        props.put("auto.offset.reset", consumerConfig.getString("auto.offset.reset"));
        consumer = new KafkaConsumer<>(props,  new StringDeserializer(),
                new StringDeserializer());
        consumer.subscribe(Collections.singletonList(consumerConfig.getString("topic")));
        checkerRules = checker;
    }
    @Override
    public void processing() {
        executor.submit(() -> {
            while(true){
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                rules = supplier.get();
                for (ConsumerRecord<String, String> MyRecord : records) {
                    Message msg = checkerRules.processing(Message.builder().value(MyRecord.value()).build(), rules);
                    writer.processing(msg);
                }
            }
        });
    }
}