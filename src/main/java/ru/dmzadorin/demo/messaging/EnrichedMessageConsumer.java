package ru.dmzadorin.demo.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import ru.dmzadorin.demo.model.EnrichedMessage;
import ru.dmzadorin.demo.model.Message;
import ru.dmzadorin.demo.service.MessageService;
import ru.dmzadorin.demo.util.JsonUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class EnrichedMessageConsumer implements ConsumerSeekAware {
    private static final Logger logger = LogManager.getLogger(EnrichedMessageConsumer.class);

    private final ObjectMapper objectMapper;
    private final String enrichedMessagesTopic;
    private final MessageService messageService;

    public EnrichedMessageConsumer(
            ObjectMapper objectMapper,
            String enrichedMessagesTopic,
            MessageService messageService
    ) {
        this.objectMapper = objectMapper;
        this.enrichedMessagesTopic = enrichedMessagesTopic;
        this.messageService = messageService;
    }

    @Override
    public void onPartitionsAssigned(
            Map<TopicPartition, Long> assignments,
            ConsumerSeekCallback callback
    ) {
        var offset = messageService.getKafkaOffset();
        if (offset != null) {
            //Need to shift offset + 1 since offset from db stores offset from last saved message
            var latestOffset = offset + 1;
            logger.info("Got kafka offset '{}' from db, rewinding enriched message consumer to offset + 1: {}",
                    offset, latestOffset);
            assignments.keySet().forEach(partition ->
                    callback.seek(enrichedMessagesTopic, partition.partition(), latestOffset)
            );
        } else {
            logger.info("Kafka offset is empty, no need to rewind enriched message consumer");
        }
    }

    @KafkaListener(
            id = "enrich-message-consumer",
            topics = "${app.enrichedMessagesTopic}",
            containerFactory = "enrichedMessageContainerFactory"
    )
    public void acceptEnrichMessages(
            @Payload List<Message> messages,
            @Header(KafkaHeaders.OFFSET) List<Long> offsets,
            Acknowledgment acknowledgment
    ) {
        logger.info("Accepted {} messages: {}", messages.size(),
                JsonUtil.writeValueAsString(messages, objectMapper));
        var enrichedMessages = new ArrayList<EnrichedMessage>(messages.size());

        for (int i = 0; i < messages.size(); i++) {
            var message = messages.get(i);
            var offset = offsets.get(i);
            enrichedMessages.add(
                    new EnrichedMessage(message.getMessageId(), message.getPayload(), offset)
            );
        }
        messageService.saveMessagePayload(enrichedMessages);
        acknowledgment.acknowledge();
    }
}
