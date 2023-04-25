package ru.mai.lessons.rpks.kafka.dispatchers;

import ru.mai.lessons.rpks.exceptions.UndefinedOperationException;

public interface DispatcherKafka {
    public void actionWithMessage(String msg) throws UndefinedOperationException;
}
