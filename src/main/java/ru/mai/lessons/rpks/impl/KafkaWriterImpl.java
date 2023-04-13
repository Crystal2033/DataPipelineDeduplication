package ru.mai.lessons.rpks.impl;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import ru.mai.lessons.rpks.KafkaWriter;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Supplier;

@Slf4j
@Builder
public final class KafkaWriterImpl implements KafkaWriter {
    private final RuleProcessor ruleProcessor;
    private final Supplier<Rule[]> rulesGetter;
    private final String topic;
    private final String bootstrapServers;

    private boolean isInitialized;

    private KafkaProducer<String, String> kafkaProducer;

    @Override
    public void processing(Message message) {
        Message checkedMessage = ruleProcessor.processing(message, rulesGetter.get());
        if (checkedMessage.isDeduplicationState()) {
            send(message);
        }
    }

    private void send(Message message) {
        if (!isInitialized) {
            init();
        }

        Future<RecordMetadata> response;
        response = kafkaProducer.send(new ProducerRecord<>(topic, message.getValue()));

        Optional.ofNullable(response).ifPresent(rsp -> {
            try {
                log.info("Message send {}", rsp.get());
            } catch (InterruptedException | ExecutionException e) {
                log.error("Error sending message ", e);
                Thread.currentThread().interrupt();
            }
        });
    }

    private void init() {
        log.info("init writerImpl");

        kafkaProducer = new KafkaProducer<>(
                Map.of(
                        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                        ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString()
                ),
                new StringSerializer(),
                new StringSerializer()
        );

        isInitialized = true;
    }
}
