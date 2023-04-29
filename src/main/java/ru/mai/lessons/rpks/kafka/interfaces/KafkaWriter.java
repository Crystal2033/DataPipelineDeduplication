package ru.mai.lessons.rpks.kafka.interfaces;

import ru.mai.lessons.rpks.model.Message;

public interface KafkaWriter {
    void processing(Message message); // отправляет сообщения с deduplicationState = true в выходной топик. Конфигурация берется из файла *.conf
}
