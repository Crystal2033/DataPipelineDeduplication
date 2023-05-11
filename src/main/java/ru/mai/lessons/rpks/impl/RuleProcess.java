package ru.mai.lessons.rpks.impl;


import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;
import org.jooq.tools.json.*;

@Slf4j
@RequiredArgsConstructor
public class RuleProcess implements RuleProcessor {
    private final RedClient redisClient;
    public Message processing(Message message, Rule[] rules) throws ParseException {
        if (rules.length == 0) return message; // нет правил
        String jsonString = message.getValue();
        jsonString =  jsonString.replace(":-", ":null");
        jsonString =  jsonString.replace(":,", ":null,");
        JSONParser jsonParser = new JSONParser();
        JSONObject jsonObject = (JSONObject) jsonParser.parse(jsonString);

        StringBuilder ruleKey = null;
        StringBuilder ruleValue = null;
        long timeSec = 0L;
        for (Rule rule : rules)
        {
            String fieldName = rule.getFieldName();
            Long liveSec = rule.getTimeToLiveSec();
            Boolean isActive = rule.getIsActive();

            if (Boolean.TRUE.equals(isActive)) {
                var value = jsonObject.get(fieldName);
                String strValue = String.valueOf(value);

                ruleKey = findKey(ruleKey, fieldName, strValue);
                ruleValue = findValue(ruleValue, strValue);

                if (timeSec < liveSec) timeSec = liveSec;
            }
        }
        if (ruleKey == null) return message;

        if (redisClient.readData(String.valueOf(ruleKey)) == null){
            redisClient.writeData(String.valueOf(ruleKey), String.valueOf(ruleValue));
            redisClient.expire(String.valueOf(ruleKey), timeSec);
        }
        else {
            message.setDeduplicationState(false);
        }

        return message;
    }
    StringBuilder findKey(StringBuilder ruleKey, String fieldName, String strValue)
    {
        if (ruleKey == null) return new StringBuilder(fieldName).append(strValue);
        else return ruleKey.append(":").append(fieldName).append(strValue);
    }
    StringBuilder findValue(StringBuilder ruleValue, String strValue)
    {
        if (ruleValue == null) return new StringBuilder(strValue);
        else return ruleValue.append(":").append(strValue);
    }
}
