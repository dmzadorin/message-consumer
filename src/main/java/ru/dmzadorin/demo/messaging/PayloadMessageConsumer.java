package ru.dmzadorin.demo.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import ru.dmzadorin.demo.model.Message;
import ru.dmzadorin.demo.model.MessagesPayload;
import ru.dmzadorin.demo.util.JsonUtil;

import java.util.List;
import java.util.stream.Collectors;

public class PayloadMessageConsumer implements ConsumerSeekAware {
    private static final Logger logger = LogManager.getLogger(PayloadMessageConsumer.class);

    private final ObjectMapper objectMapper;
    private final String enrichedMessagesTopic;
    private final KafkaTemplate<Long, Message> enrichedMessagesTemplate;

    public PayloadMessageConsumer(
            ObjectMapper objectMapper,
            String enrichedMessagesTopic,
            KafkaTemplate<Long, Message> enrichedMessagesTemplate
    ) {
        this.objectMapper = objectMapper;
        this.enrichedMessagesTopic = enrichedMessagesTopic;
        this.enrichedMessagesTemplate = enrichedMessagesTemplate;
    }

    @KafkaListener(
            id = "message-payload-consumer",
            topics = "${app.payloadMessagesTopic}",
            containerFactory = "payloadMessageContainerFactory"
    )
    public void acceptMessagesPayload(
            @Payload List<MessagesPayload> messages,
            Acknowledgment acknowledgment
    ) {
        logger.info("Accepted new message payload from kafka: {}",
                JsonUtil.writeValueAsString(messages, objectMapper)
        );
        if (messages.isEmpty()) {
            logger.warn("Messages payload batch is empty");
        }
        var filtered = messages.stream()
                .filter(this::filterPayload)
                .flatMap(payload -> payload.getMessages().stream().filter(this::filterMessage))
                .collect(Collectors.toList());

        filtered.forEach(message ->
                enrichedMessagesTemplate.send(
                        enrichedMessagesTopic,
                        message.getMessageId(),
                        message
                ));
        acknowledgment.acknowledge();
    }

    private boolean filterPayload(MessagesPayload payload) {
        boolean filterPayload = !payload.getMessages().isEmpty();
        if (!filterPayload) {
            logger.warn("Message payload does not contain messages");
        }
        return filterPayload;
    }

    private boolean filterMessage(Message message) {
        var correctMessage = message.getMessageId() != null && message.getMessageId() >= 0;
        if (!correctMessage) {
            logger.warn(
                    "Message with id {} is skipped, id is less than 0", message.getMessageId()
            );
        }
        return correctMessage;
    }
}
