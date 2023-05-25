package ru.mai.lessons.rpks.impl;

import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.RedisClient;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Slf4j
@RequiredArgsConstructor
public final class RuleProcessorImpl implements RuleProcessor {
    private final RedisClient redis;

    @Override
    public Message processing(Message message, Rule[] rules) {
        if ((rules == null) || (rules.length == 0)) {
            message.setDeduplicationState(true);
            return message;
        }

        List<String> names = Arrays.stream(rules).filter(Rule::getIsActive).map(Rule::getFieldName).sorted().toList();
        if (names.isEmpty()) {
            message.setDeduplicationState(true);
            return message;
        }

        List<String> values = getValues(message, names);
        if (values.isEmpty()) {
            message.setDeduplicationState(false);
            return message;
        }

        String joinedNames = joinStrings(names);
        String joinedValues = joinStrings(values);

        if (!redis.containsKey(joinedValues)) {
            message.setDeduplicationState(true);
            redis.write(joinedValues, joinedNames, getTime(rules));
            return message;
        }

        if (redis.read(joinedValues).equals(joinedNames))
            message.setDeduplicationState(false);
        else {
            redis.write(joinedValues, joinedNames, getTime(rules));
            message.setDeduplicationState(true);
        }
        return message;
    }

    private List<String> getValues(Message message, List<String> activeFieldNames) {
        try {
            JsonObject jsonObject = JsonParser.parseString(message.getValue()).getAsJsonObject();
            return activeFieldNames.stream()
                    .map(fieldName -> jsonObject.get(fieldName).toString())
                    .toList();
        } catch (JsonParseException | IllegalStateException e) {
            return new ArrayList<>();
        }
    }

    private String joinStrings(List<String> strings) {
        StringBuilder builder = new StringBuilder();
        strings.forEach(string -> {
            if (builder.length() != 0)
                builder.append('-');
            builder.append(string);
        });
        return builder.toString();
    }

    private long getTime(Rule[] rules) {
        return Arrays.stream(rules)
                .filter(Rule::getIsActive)
                .mapToLong(Rule::getTimeToLiveSec)
                .max()
                .orElse(0);
    }
}
