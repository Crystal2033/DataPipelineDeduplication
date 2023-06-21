package ru.mai.lessons.rpks.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@AllArgsConstructor
@Builder
public class Message {
    private String value; // сообщение из Kafka в формате JSON

    private boolean deduplicationState; // true - удовлетворены условиях всех правил (Rule), false - хотя бы одно условие не прошло проверку.
}
