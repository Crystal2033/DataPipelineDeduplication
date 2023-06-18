package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mai.lessons.rpks.DeduplicationRedisService;
import ru.mai.lessons.rpks.DeduplicationRuleCreator;
import ru.mai.lessons.rpks.RedisClient;
import ru.mai.lessons.rpks.model.DeduplicationResult;
import ru.mai.lessons.rpks.model.Rule;

@Slf4j
public class DeduplicationRedisServiceImpl implements DeduplicationRedisService {
    private final DeduplicationRuleCreator deduplicationRuleCreator;
    private final RedisClient redisClient;

    public DeduplicationRedisServiceImpl(Config config) {
       this.deduplicationRuleCreator = new DeduplicationRuleCreatorImpl();
       this.redisClient = new RedisClientImpl(config);
    }

    public boolean deduplicate(ConsumerRecord<String, String> consumerRecord, Rule[] rules) {
        var state = deduplicationRuleCreator.apply(consumerRecord, rules);
        if (state.result() == DeduplicationResult.FAILED) {
            return false;
        } else if (state.result() == DeduplicationResult.NO_RULES) {
            return true;
        }
        if (!redisClient.exist(state.key())) {
            log.debug("Push {} ttl {}", state.key(), state.timeToLiveSec());
            redisClient.set(state.key(), "");
            redisClient.expire(state.key(), state.timeToLiveSec());
            return true;
        }
        log.debug("Existed key {}", state.key());
        return false;
    }
}
