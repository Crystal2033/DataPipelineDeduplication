package ru.mai.lessons.rpks.kafka.interfaces;

import ru.mai.lessons.rpks.exceptions.UndefinedOperationException;

public interface DispatcherKafka {
    public void actionWithMessage(String msg) throws UndefinedOperationException;
}
