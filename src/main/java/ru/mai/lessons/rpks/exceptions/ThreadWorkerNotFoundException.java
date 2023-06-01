package ru.mai.lessons.rpks.exceptions;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class ThreadWorkerNotFoundException extends Exception {
    private final String message;
}
