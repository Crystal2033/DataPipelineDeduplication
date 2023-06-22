package ru.mai.lessons.rpks.impl;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.util.concurrent.ConcurrentLinkedQueue;

@Data
@Slf4j
public class MessageHandler {

    private final KafkaWriterImpl producer;
    private final RuleProcessorImpl ruleProcessor;
    private final ConcurrentLinkedQueue<Rule> rules;

    void processMessage(Message message) {
        Rule[] ruleArr = new Rule[rules.size()];
        ruleProcessor.processing(message, rules.toArray(ruleArr));
        if (message.isDeduplicationState())
            producer.processing(message);
    }
}