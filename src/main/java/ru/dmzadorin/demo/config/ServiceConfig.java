package ru.dmzadorin.demo.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;
import ru.dmzadorin.demo.db.MessageRepository;
import ru.dmzadorin.demo.messaging.PartitionOffsetRewinder;
import ru.dmzadorin.demo.service.MessageService;
import ru.dmzadorin.demo.service.MessageServiceImpl;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Configuration
public class ServiceConfig {

    @Bean
    public MessageService messageService(
            MessageRepository messageRepository,
            @Value("${messages.batchSize}") int batchSize,
            @Value("${messages.maxWaitTimeout}") long waitTimeout,
            @Value("${messages.maxWaitTimeUnit}") TimeUnit timeUnit,
            ScheduledExecutorService messageScheduledExecutor
    ) {
        return new MessageServiceImpl(
                messageRepository,
                batchSize,
                waitTimeout,
                timeUnit,
                messageScheduledExecutor
        );
    }

    @Bean
    ScheduledExecutorService messageScheduledExecutor(@Value("${messages.poolSize}") int poolSize) {
        var threadFactory = new CustomizableThreadFactory("queue-poller-");
        return Executors.newScheduledThreadPool(poolSize, threadFactory);
    }
}
