package ru.dmzadorin.demo.db;

import ru.dmzadorin.demo.model.Message;

import java.util.Collection;

public interface MessagesRepository {

    void saveOne(Message message);

    void saveBatch(Collection<Message> messages);
}
