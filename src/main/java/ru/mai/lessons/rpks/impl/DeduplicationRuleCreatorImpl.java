package ru.mai.lessons.rpks.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import ru.mai.lessons.rpks.DeduplicationRuleCreator;
import ru.mai.lessons.rpks.model.DeduplicationResult;
import ru.mai.lessons.rpks.model.Rule;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;


@Slf4j
public class DeduplicationRuleCreatorImpl implements DeduplicationRuleCreator {
    private final ObjectMapper objectMapper;

    public DeduplicationRuleCreatorImpl() {
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public DeduplicationState apply(ConsumerRecord<String, String> consumerRecord, Rule[] rules) {
        log.debug("Check rules for record {}", consumerRecord.value());
        if (rules == null || rules.length == 0) {
            return new DeduplicationState(null, null, DeduplicationResult.NO_RULES);
        }

        List<Rule> activeRuleList = Stream.of(rules)
                .filter(Rule::getIsActive)
                .toList();

        if (activeRuleList.isEmpty()) {
            return new DeduplicationState(null, null, DeduplicationResult.NO_RULES);
        }

        ObjectNode objectNode;
        try {
            objectNode = objectMapper.readValue(consumerRecord.value(), new TypeReference<>() {
            });
        } catch (JsonProcessingException e) {
            log.error("Не смогли создать json", e);
            return new DeduplicationState(null, null, DeduplicationResult.FAILED);
        }

        String deduplicationKey = activeRuleList
                .stream()
                .map(r -> apply(objectNode, r))
                .collect(Collectors.joining("_"));

        Long timeToLiveSec = activeRuleList
                .stream()
                .map(Rule::getTimeToLiveSec)
                .min(Comparator.naturalOrder()).orElse(null);

        return new DeduplicationState(deduplicationKey, timeToLiveSec, DeduplicationResult.SUCCESS);
    }

    private String apply(ObjectNode objectNode, Rule rule) {
        var fieldNameNode = objectNode.get(rule.getFieldName());
        if (rule.getFieldName() == null) {
            return null;
        }

        return rule.getFieldName() + "_" + fieldNameNode.asText();
    }


    public record DeduplicationState(String key, Long timeToLiveSec, DeduplicationResult result) {
    }
}

