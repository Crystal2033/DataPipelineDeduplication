package ru.mai.lessons.rpks.model;

import lombok.Data;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@Data
@RequiredArgsConstructor
public class Message {
    @NonNull
    private String value; // сообщение из Kafka в формате JSON
    private boolean deduplicationState = false; // true - удовлетворены условиях всех правил (Rule), false - хотя бы одно условие не прошло проверку.
}
