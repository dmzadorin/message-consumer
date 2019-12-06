package ru.dmzadorin.demo.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import ru.dmzadorin.demo.db.MessageRepository;
import ru.dmzadorin.demo.messaging.PartitionOffsetRewinder;
import ru.dmzadorin.demo.model.DbTemporaryUnavailable;
import ru.dmzadorin.demo.model.EnrichedMessage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;

public class MessageServiceImpl implements MessageService {
    private static final Logger logger = LogManager.getLogger(MessageServiceImpl.class);

    private final MessageRepository messageRepository;
    private final int batchSize;
    private final ScheduledExecutorService executorService;
    private final Lock queueSynchronizer;
    private PartitionOffsetRewinder partitionOffsetRewinder;

    private final BlockingQueue<EnrichedMessage> messageQueue;

    public MessageServiceImpl(
            MessageRepository messageRepository,
            int batchSize,
            long waitTimeout,
            TimeUnit timeUnit,
            ScheduledExecutorService executorService
    ) {
        this.messageRepository = messageRepository;
        this.batchSize = batchSize;
        this.executorService = executorService;
        this.messageQueue = new LinkedBlockingQueue<>();
        this.queueSynchronizer = new ReentrantLock();
        executorService.scheduleWithFixedDelay(
                this::collectMessages, waitTimeout, waitTimeout, timeUnit
        );
    }

    @Override
    public void saveMessagePayload(Collection<EnrichedMessage> enrichedMessages) {
        saveMessagesToQueue(enrichedMessages);
    }

    @Override
    public Long getPartitionOffset(int partition) {
        return messageRepository.getPartitionOffset(partition);
    }

    @Autowired
    public void setPartitionOffsetRewinder(PartitionOffsetRewinder partitionOffsetRewinder) {
        this.partitionOffsetRewinder = partitionOffsetRewinder;
    }

    private void saveMessagesToQueue(Collection<EnrichedMessage> enrichedMessages) {
        logger.info("Saving {} messages to queue", enrichedMessages.size());
        for (var message : enrichedMessages) {
            try {
                messageQueue.put(message);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        var messagesSize = messageQueue.size();
        if (messagesSize >= batchSize) {
            logger.info("Message queue size ({}) is greater than or equal to batch size ({}), " +
                    "running save to database", messagesSize, batchSize
            );
            executorService.submit(this::collectMessages);
        }
    }

    private void collectMessages() {
        try {
            queueSynchronizer.lock();
            logger.info("Collecting messages from message queue");
            var messages = new ArrayList<EnrichedMessage>(messageQueue.size());
            messageQueue.drainTo(messages);

            if (messages.isEmpty()) {
                logger.debug("Message queue is empty");
            } else {
                logger.info("Got {} messages from queue, saving to db", messages.size());
                try {
                    messageRepository.saveBatch(messages);
                    logger.info("Batch with {} messages successfully saved to db", messages.size());
                } catch (DbTemporaryUnavailable e) {
                    logger.error("{}, need to rewind partition offsets", e.getMessage());
                    rewindPartitionOffset(messages);
                }
            }
        } finally {
            queueSynchronizer.unlock();
        }
    }

    private void rewindPartitionOffset(Collection<EnrichedMessage> messages) {
        var partitionToOffset = messages.stream()
                .collect(
                        Collectors.toMap(
                                EnrichedMessage::getPartition,
                                EnrichedMessage::getOffset,
                                BinaryOperator.minBy(Long::compareTo)
                        )
                );
        partitionOffsetRewinder.rewindPartitionOffset(partitionToOffset);
    }
}
