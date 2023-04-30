package ru.mai.lessons.rpks.model;


import lombok.Data;
import lombok.RequiredArgsConstructor;

@Data
@RequiredArgsConstructor
public class Message {
    private final String value; // сообщение из Kafka в формате JSON
    private boolean deduplicationState = false; // true - удовлетворены условиях всех правил (Rule), false - хотя бы одно условие не прошло проверку.
}
