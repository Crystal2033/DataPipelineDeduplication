package ru.mai.lessons.rpks.repository.impl;

import com.typesafe.config.Config;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.model.Rule;

import java.util.*;

@RequiredArgsConstructor
@Slf4j
@Getter
public class RulesUpdaterThread implements Runnable {

    private List<Rule> rules;

    private final DataBaseReader dataBaseReader;

    private final Config configForSleep;

    private List<Rule> makeUniqueListWithGreatestTTL(List<Rule> rulesFromDB) {
        Map<String, Rule> greatestTTLRulesMap = new HashMap<>();
        for (Rule rule : rulesFromDB) {
            if (!greatestTTLRulesMap.containsKey(rule.getFieldName())) {
                greatestTTLRulesMap.put(rule.getFieldName(), rule);
            } else {
                if (greatestTTLRulesMap.get(rule.getFieldName()).getTimeToLiveSec() < rule.getTimeToLiveSec()) {
                    greatestTTLRulesMap.put(rule.getFieldName(), rule);
                }
            }
        }
        return new ArrayList<>(greatestTTLRulesMap.values());
    }

    @Override
    public void run() {
        rules = makeUniqueListWithGreatestTTL(List.of(dataBaseReader.readRulesFromDB()));
        rules.sort(Comparator.comparing(Rule::getFieldName));
    }
}
