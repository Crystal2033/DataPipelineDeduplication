package ru.mai.lessons.rpks.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.model.Rule;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

@RequiredArgsConstructor
@Slf4j
public class RulesUpdaterThread implements Runnable {

    private final DbReaderImpl dbReader;
    private final ConcurrentLinkedQueue<Rule> rulesConcurrent;

    @Override
    public void run() {
        Rule[] rules = dbReader.readRulesFromDB();
        rulesConcurrent.clear();
        rulesConcurrent.addAll(List.of(rules));
    }
}