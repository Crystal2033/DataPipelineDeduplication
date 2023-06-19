package ru.mai.lessons.rpks.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.util.Objects;

@Slf4j
public class RuleProcessorImpl implements RuleProcessor {
    ObjectMapper mapper = new ObjectMapper();

    boolean compare(String value, Rule rule){
//        if (Objects.equals(rule.getFilterFunctionName(), "equals")
//                && Objects.equals(value, rule.getFilterValue())) {
//            return true;
//        }
//        else if (Objects.equals(rule.getFilterFunctionName(), "contains")
//                && value.contains(rule.getFilterValue())) {
//            return true;
//        }
//        else if (Objects.equals(rule.getFilterFunctionName(), "not_equals")
//                && !Objects.equals(value, rule.getFilterValue())) {
//            return true;
//        }
//        else return Objects.equals(rule.getFilterFunctionName(), "not_contains")
//                    && !value.contains(rule.getFilterValue());
        return true;
    }

    @Override
    public Message processing(Message message, Rule[] rules) {

        try {
            JsonNode jsonNode = mapper.readTree(message.getValue());
            for (var rule : rules) {
                JsonNode temp = jsonNode.get(rule.getFieldName());
                if (temp == null)
                {
                    message.setDeduplicationState(false);
                    return message;
                }

                if (!temp.isValueNode())
                {
                    message.setDeduplicationState(false);
                    return message;
                }

                String value = jsonNode.get(rule.getFieldName()).asText();
                if (value == null)
                {
                    message.setDeduplicationState(false);
                    return message;
                }
                if (value.isEmpty())
                {
                    message.setDeduplicationState(false);
                    return message;
                }
                if (!compare(value, rule)) {
                    message.setDeduplicationState(false);
                    return message;
                }

                message.setDeduplicationState(true);
            }
        } catch (Exception e){
            log.info("Json error :{}", e.toString());
        }

        return message;
    }
}
