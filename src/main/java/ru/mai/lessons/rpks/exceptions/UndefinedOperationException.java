package ru.mai.lessons.rpks.exceptions;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class UndefinedOperationException extends Exception {
    private final String msg;
    private final String operation;
}
