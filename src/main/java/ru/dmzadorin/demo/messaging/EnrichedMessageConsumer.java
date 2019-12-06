package ru.dmzadorin.demo.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AbstractConsumerSeekAware;
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
import java.util.stream.Collectors;

public class EnrichedMessageConsumer extends AbstractConsumerSeekAware implements PartitionOffsetRewinder {
    private static final Logger logger = LogManager.getLogger(EnrichedMessageConsumer.class);

    private final ObjectMapper objectMapper;
    private final String enrichedMessagesTopic;
    private MessageService messageService;

    public EnrichedMessageConsumer(
            ObjectMapper objectMapper,
            String enrichedMessagesTopic
    ) {
        this.objectMapper = objectMapper;
        this.enrichedMessagesTopic = enrichedMessagesTopic;
    }

    @Autowired
    public void setMessageService(MessageService messageService) {
        this.messageService = messageService;
    }

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        super.onPartitionsAssigned(assignments, callback);
        //Rewind partitions to correct offset
        assignments.forEach((partition, offset) -> seekPartition(callback, partition, offset));
    }

    @KafkaListener(
            id = "enrich-message-consumer",
            topics = "${app.enrichedMessagesTopic}",
            containerFactory = "enrichedMessageContainerFactory"
    )
    public void acceptEnrichMessages(
            @Payload List<Message> messages,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) List<Integer> partitions,
            @Header(KafkaHeaders.OFFSET) List<Long> offsets,
            Acknowledgment acknowledgment
    ) {
        logger.info("Accepted {} messages: {}", messages.size(),
                JsonUtil.writeValueAsString(messages, objectMapper));
        var enrichedMessages = new ArrayList<EnrichedMessage>(messages.size());

        for (int i = 0; i < messages.size(); i++) {
            var message = messages.get(i);
            var offset = offsets.get(i);
            var partition = partitions.get(i);
            enrichedMessages.add(
                    new EnrichedMessage(message.getMessageId(), message.getPayload(), partition, offset)
            );
        }
        messageService.saveMessagePayload(enrichedMessages);
        acknowledgment.acknowledge();
    }

    @Override
    public void rewindPartitionOffset(Map<Integer, Long> partitionsOffset) {
        var partitionToSeekCallback = getSeekCallbacks().entrySet().stream()
                .collect(Collectors.toMap(e -> e.getKey().partition(), Map.Entry::getValue));
        logger.info("Need to rewind partitions to specific offsets: {}", partitionsOffset);
        partitionsOffset.forEach((partition, offset) -> {
            var callback = partitionToSeekCallback.get(partition);
            if (callback != null) {
                doSeekPartition(partition, offset, callback);
            }
        });
        logger.info("Partition rewind finished");
    }

    private void seekPartition(
            ConsumerSeekCallback callback,
            TopicPartition partition,
            Long currentOffset
    ) {
        var offset = messageService.getPartitionOffset(partition.partition());
        if (offset != null) {
            //Need to shift offset + 1 since offset from db stores offset from last saved message
            var latestOffset = offset + 1;
            if (currentOffset != null && currentOffset == latestOffset) {
                logger.info("No need to rewind message listener for partition '{}', " +
                                "latest offset from db is equal to current offset: {}",
                        partition, currentOffset
                );
            } else {
                logger.info("Offset '{}' for partition '{}' from db is not equal to current offset: '{}'," +
                                " rewinding listener",
                        latestOffset, partition, currentOffset);
                doSeekPartition(partition.partition(), latestOffset, callback);
            }
        } else {
            logger.info("Offset for partition '{}' is empty in db, no need to rewind listener offset",
                    partition);
        }
    }

    private void doSeekPartition(Integer partition, Long offset, ConsumerSeekCallback callback) {
        logger.info("Rewinding partition '{}' to offset: '{}'", partition, offset);
        callback.seek(enrichedMessagesTopic, partition, offset);
    }
}
