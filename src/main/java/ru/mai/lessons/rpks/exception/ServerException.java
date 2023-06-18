package ru.mai.lessons.rpks.exception;

public class ServerException extends RuntimeException {

    public ServerException(String msg) {
        super(msg);
    }

    public ServerException(String msg, Throwable e) {
        super(msg, e);
    }
}
