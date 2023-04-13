package ru.mai.lessons.rpks.impl;

import lombok.RequiredArgsConstructor;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.util.Map;
import java.util.function.BiPredicate;

@RequiredArgsConstructor
public final class RuleProcessorImpl implements RuleProcessor {
    private static final Map<String, BiPredicate<String, String>> functionNameAndItsBiPredicate = Map.of(
            "equals", (fieldValue, filterValue) -> filterValue.equals(fieldValue),
            "contains", (fieldValue, filterValue) -> fieldValue != null && fieldValue.contains(filterValue),
            "not_equals", (fieldValue, filterValue) -> fieldValue != null && !fieldValue.equals("") && !filterValue.equals(fieldValue),
            "not_contains", (fieldValue, filterValue) -> fieldValue != null && !fieldValue.equals("") && !fieldValue.contains(filterValue)
    );

    @Override
    public Message processing(Message message, Rule[] rules) {
        if (rules == null || rules.length == 0) {
            message.setDeduplicationState(false);
            return message;
        }

        for (Rule rule : rules) {
            if (!setMessageState(message, rule)) {
                break;
            }
        }
        return message;
    }

    private boolean setMessageState(Message message, Rule rule) {
        return true;
    }

    private String getFieldValueFromJSON(String jsonString, String fieldName) {
        return "";
    }
}
