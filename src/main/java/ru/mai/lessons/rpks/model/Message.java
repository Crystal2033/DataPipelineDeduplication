package ru.mai.lessons.rpks.model;

import lombok.*;

@Data
@Builder
@RequiredArgsConstructor
public class Message {
    @NonNull
    private String value; // сообщение из Kafka в формате JSON
    @NonNull
    private boolean deduplicationState; // true - удовлетворены условиях всех правил (Rule), false - хотя бы одно условие не прошло проверку.
}
