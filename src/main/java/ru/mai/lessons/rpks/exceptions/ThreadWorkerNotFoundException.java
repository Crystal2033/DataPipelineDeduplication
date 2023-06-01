package ru.mai.lessons.rpks.exceptions;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class ThreadWorkerNotFoundException extends Exception{
    private final String msg;
}
