package ru.dmzadorin.demo.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import ru.dmzadorin.demo.db.MessagesRepository;
import ru.dmzadorin.demo.model.Message;
import ru.dmzadorin.demo.model.MessagesPayload;

import java.util.List;
import java.util.stream.Collectors;

public class KafkaMessageConsumer {
    private static final Logger logger = LogManager.getLogger(KafkaMessageConsumer.class);

    private final ObjectMapper objectMapper;
    private final MessagesRepository messagesRepository;

    public KafkaMessageConsumer(
            ObjectMapper objectMapper,
            MessagesRepository messagesRepository
    ) {
        this.objectMapper = objectMapper;
        this.messagesRepository = messagesRepository;
    }

    @KafkaListener(id = "message-consumer", topics = "${messages.topic}")
    public void accept(
            MessagesPayload messages,
            @Header(KafkaHeaders.RECEIVED_TOPIC) final List<String> topics,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) final List<Integer> partitionIds,
            @Header(KafkaHeaders.RECEIVED_TIMESTAMP) final List<Long> timestamps,
            @Header(KafkaHeaders.OFFSET) final List<Long> offsets
    ) {
        logger.info("Accepted new message payload from kafka: " + writeValueAsString(messages));
        if (messages.getMessages().isEmpty()) {
            logger.warn("Messages payload is empty");
        } else {
            var filteredMessages = messages.getMessages().stream()
                    .filter(this::filterMessage)
                    .collect(Collectors.toList());
            messagesRepository.saveBatch(filteredMessages);

        }
    }

    private boolean filterMessage(Message m) {
        boolean correctMessage = m.getMessageId() != null && m.getMessageId() >= 0;
        if (!correctMessage) {
            logger.warn("Message with id {} is not saved to db", m.getMessageId());
        }
        return correctMessage;
    }

    private String writeValueAsString(MessagesPayload dto) {
        try {
            return objectMapper.writeValueAsString(dto);
        } catch (JsonProcessingException e) {
            logger.error("Writing value to JSON failed: ", e);
            return "failed to convert";
        }
    }
}
