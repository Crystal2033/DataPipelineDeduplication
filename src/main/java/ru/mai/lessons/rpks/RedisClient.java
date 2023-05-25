package ru.mai.lessons.rpks;

public interface RedisClient {
    String read(String key);
    void write(String key, String value, long seconds);
    boolean containsKey(String key);
}
