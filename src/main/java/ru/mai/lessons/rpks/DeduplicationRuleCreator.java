package ru.mai.lessons.rpks;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import ru.mai.lessons.rpks.impl.DeduplicationRuleCreatorImpl;
import ru.mai.lessons.rpks.model.Rule;

public interface DeduplicationRuleCreator {
    DeduplicationRuleCreatorImpl.DeduplicationState apply(ConsumerRecord<String, String> consumerRecord, Rule[] rules); // Применяет правила дедубликации к сообщениям и устанавливает в них deduplicationState значение true, если сообщение удовлетворяет условиям всех правил. Несколько правил объединяются в один ключ, значит если несколько правил, то из них составляет один ключ и одним запросом проверяется в Redis. Если у правил разное время, то берётся большее из них.
}
