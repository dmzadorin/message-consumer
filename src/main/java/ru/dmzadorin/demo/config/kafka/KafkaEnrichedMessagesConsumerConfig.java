package ru.dmzadorin.demo.config.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import ru.dmzadorin.demo.messaging.EnrichedMessageConsumer;
import ru.dmzadorin.demo.model.Message;

import javax.annotation.Resource;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaEnrichedMessagesConsumerConfig {

    @Value("${app.enrichedMessagesTopic}")
    private String enrichedMessagesTopic;

    @Value("${app.enrichedMessagesListeners}")
    private int enrichedMessagesListeners;

    @Resource(name = "commonConsumerFactoryProperties")
    private Map<String, Object> commonProperties;

    @Bean
    public ConcurrentKafkaListenerContainerFactory<Long, Message> enrichedMessageContainerFactory(
            ConsumerFactory<Long, Message> batchConsumerFactory
    ) {
        ConcurrentKafkaListenerContainerFactory<Long, Message> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(batchConsumerFactory);
        factory.setConcurrency(enrichedMessagesListeners);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setBatchListener(true);
        return factory;
    }

    @Bean
    public ConsumerFactory<Long, Message> batchConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(enrichedMessageConsumerConfigs());
    }

    @Bean
    public Map<String, Object> enrichedMessageConsumerConfigs() {
        Map<String, Object> props = new HashMap<>(commonProperties);
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, Message.class);
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);

        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "Enriched-Message-Consumer-Group");

        return props;
    }

    @Bean
    public EnrichedMessageConsumer enrichedMessagesConsumer(
            ObjectMapper mapper
    ) {
        return new EnrichedMessageConsumer(mapper, enrichedMessagesTopic);
    }
}
