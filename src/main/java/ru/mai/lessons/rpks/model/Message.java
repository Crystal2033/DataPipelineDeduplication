package ru.mai.lessons.rpks.model;

import lombok.Builder;
import lombok.Data;
import lombok.NonNull;

@Data
@Builder
public class Message {
    @NonNull
    private String value; // сообщение из Kafka в формате JSON

    private boolean deduplicationState; // true - удовлетворены условиях всех правил (Rule), false - хотя бы одно условие не прошло проверку.
}
