package ru.mai.lessons.rpks;

import ru.mai.lessons.rpks.model.Rule;

import java.sql.SQLException;


public interface DbReader {
    Rule[] readRulesFromDB() throws SQLException; // метод получает набор правил из БД PostgreSQL. Конфигурация для подключения из файла *.conf. Метод также должен проверять в заданное время с периодом изменения в БД и обновлять правила.
}
