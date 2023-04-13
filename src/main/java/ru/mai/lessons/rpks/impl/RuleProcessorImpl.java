package ru.mai.lessons.rpks.impl;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import ru.mai.lessons.rpks.RedisClient;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

@RequiredArgsConstructor
public final class RuleProcessorImpl implements RuleProcessor {
    private final RedisClient redisClient;

    @Override
    public Message processing(Message message, Rule[] rules) {
        if (rules == null || rules.length == 0) {
            message.setDeduplicationState(false);
            return message;
        }

        CombinedRule combinedRule = combineRules(rules);
        setMessageState(message, combinedRule);
        return message;
    }

    private CombinedRule combineRules(Rule[] rules) {
        long maxTimeToLiveSec = 0;
        StringBuilder stringBuilderToCombineRules = new StringBuilder();

        for (Rule rule : rules) {
            boolean ruleIsActive = rule.getIsActive();
            if (!ruleIsActive) {
                continue;
            }

            maxTimeToLiveSec = Math.max(maxTimeToLiveSec, rule.getTimeToLiveSec());
            if (stringBuilderToCombineRules.length() != 0) {
                stringBuilderToCombineRules.append(':');
            }
            stringBuilderToCombineRules.append(rule.getRuleId());
        }
        return new CombinedRule(stringBuilderToCombineRules.toString(), maxTimeToLiveSec);
    }

    private void setMessageState(Message message, CombinedRule combinedRule) {
        if (!combinedRule.isActive()) {
            message.setDeduplicationState(true);
            return;
        }
        if (redisClient.containsKey(combinedRule.getCombinedRules())) {
            message.setDeduplicationState(false);
            return;
        }
        redisClient.writeData(combinedRule.toString(), message.getValue(), combinedRule.getTimeToLiveSec());
    }

    @Data
    private static class CombinedRule {
        private final String combinedRules;
        private final long timeToLiveSec;

        public boolean isActive() {
            return combinedRules.length() > 0;
        }
    }
}
