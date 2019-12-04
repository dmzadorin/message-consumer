package ru.dmzadorin.demo.service;

import ru.dmzadorin.demo.model.EnrichedMessage;

import java.util.Collection;

public interface MessageService {
    void saveMessagePayload(Collection<EnrichedMessage> enrichedMessages);

    Long getKafkaOffset();
}
