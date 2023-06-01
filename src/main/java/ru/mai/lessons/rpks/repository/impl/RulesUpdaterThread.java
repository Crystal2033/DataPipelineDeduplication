package ru.mai.lessons.rpks.repository.impl;

import com.typesafe.config.Config;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.model.Rule;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@RequiredArgsConstructor
@Slf4j
@Getter
public class RulesUpdaterThread implements Runnable {

    private final ConcurrentHashMap<String, List<Rule>> rulesConcurrentMap;

    private final DataBaseReader dataBaseReader;

    private final Config configForSleep;

    private void insertNewRulesInMap(Rule[] rules) {
        rulesConcurrentMap.clear();
        for (var rule : rules) {
            List<Rule> myList;
            if ((myList = rulesConcurrentMap.get(rule.getFieldName())) != null) {
                myList.add(rule);
                continue;
            }
            myList = new ArrayList<>();
            myList.add(rule);
            rulesConcurrentMap.put(rule.getFieldName(), myList);
        }
    }

    @Override
    public void run() {
        Rule[] rules = dataBaseReader.readRulesFromDB();
        insertNewRulesInMap(rules);
    }
}
