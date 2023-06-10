package ru.mai.lessons.rpks;

import java.util.Map;

public interface RedisClient {
    /** Нужно реализовать этот интерфейс таким образом:
     Чтение данных по ключу, чтобы проверить есть ли уже такой ключ в Redis.
     Если есть, значит это дулю и устанавливаем deduplicationState = false.
     Если нет, значит вставляем это значение в Redis, устанавливаем время жизни сообщения по правилу из PostgreSQL и проставляем deduplicationState = true.
     Реализация RedisClient должна работать в RuleProcessor.
    */
    public boolean containsKey(Map<String, String> key);

    public void write(Map<String, String> key, Map<String, String> value, long timeToLiveSec);
}
