package ru.dmzadorin.demo.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.dmzadorin.demo.db.MessageRepository;
import ru.dmzadorin.demo.model.EnrichedMessage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MessageServiceImpl implements MessageService {
    private static final Logger logger = LogManager.getLogger(MessageServiceImpl.class);

    private final MessageRepository messageRepository;
    private final int batchSize;
    private final ScheduledExecutorService executorService;

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
            logger.info("Message queue size ({}) is greater than batch size ({}), " +
                    "running save to database", messagesSize, batchSize
            );
            executorService.submit(this::collectMessages);
        }
    }

    private void collectMessages() {
        logger.info("Collecting messages from message queue");
        var messages = new ArrayList<EnrichedMessage>(messageQueue.size());
        messageQueue.drainTo(messages);
        logger.info("Got {} messages from queue, saving to db", messages.size());
        if (!messages.isEmpty()) {
                messageRepository.saveBatch(messages);
                logger.info("Batch with {} messages successfully saved to db", messages.size());
        }
    }
}
