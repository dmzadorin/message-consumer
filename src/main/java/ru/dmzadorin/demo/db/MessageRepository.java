package ru.dmzadorin.demo.db;

import ru.dmzadorin.demo.model.DbTemporaryUnavailable;
import ru.dmzadorin.demo.model.EnrichedMessage;

import java.util.Collection;

public interface MessageRepository {

    void saveBatch(Collection<EnrichedMessage> messages) throws DbTemporaryUnavailable;

    Long getPartitionOffset(int partition);
}
