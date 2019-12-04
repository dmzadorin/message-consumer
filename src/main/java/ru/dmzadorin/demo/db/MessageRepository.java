package ru.dmzadorin.demo.db;

import ru.dmzadorin.demo.model.EnrichedMessage;
import ru.dmzadorin.demo.model.Message;

import java.util.Collection;

public interface MessageRepository {

    void saveBatch(Collection<EnrichedMessage> messages);

    Long getKafkaOffset();
}
