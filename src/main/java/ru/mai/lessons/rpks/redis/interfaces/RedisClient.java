package ru.mai.lessons.rpks.redis.interfaces;

import ru.mai.lessons.rpks.model.Message;

public interface RedisClient {
    Message getMessageByStringAndTryToInsertInRedis(String stringMessage, String key, long expireTimeInSec);
    /** Нужно реализовать этот интерфейс таким образом:
     Чтение данных по ключу, чтобы проверить есть ли уже такой ключ в Redis.
     Если есть, значит это дубль и устанавливаем deduplicationState = false.
     Если нет, значит вставляем это значение в Redis, устанавливаем время жизни
     сообщения по правилу из PostgreSQL и проставляем deduplicationState = true.
     Реализация RedisClient должна работать в RuleProcessor.
     */
}
