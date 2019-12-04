package ru.dmzadorin.demo.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.converter.BatchMessagingMessageConverter;
import org.springframework.kafka.support.converter.BytesJsonMessageConverter;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer2;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import ru.dmzadorin.demo.messaging.EnrichedMessageConsumer;
import ru.dmzadorin.demo.model.Message;
import ru.dmzadorin.demo.service.MessageService;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaEnrichedMessagesConsumerConfig {

    @Value("${spring.kafka.consumer.bootstrap-servers}")
    private String kafkaServer;

    @Value("${app.enrichedMessagesTopic}")
    private String enrichedMessagesTopic;

    @Autowired
    private BytesJsonMessageConverter messageConverter;

    @Bean
    public ConcurrentKafkaListenerContainerFactory<Long, Message> enrichedMessageContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<Long, Message> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(batchConsumerFactory());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setMessageConverter(new BatchMessagingMessageConverter(messageConverter));
        factory.setBatchListener(true);
        return factory;
    }

    @Bean
    public ConsumerFactory<Long, Message> batchConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(batchConsumerConfigs());
    }

    @Bean
    public Map<String, Object> batchConsumerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer2.class);
        props.put(ErrorHandlingDeserializer2.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);

        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        props.put(ConsumerConfig.GROUP_ID_CONFIG, "Message-Consumer-Group");
        return props;
    }

    @Bean
    public EnrichedMessageConsumer enrichedMessagesConsumer(
            ObjectMapper mapper,
            MessageService messageService
    ) {
        return new EnrichedMessageConsumer(mapper, enrichedMessagesTopic, messageService);
    }
}
