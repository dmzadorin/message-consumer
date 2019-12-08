package ru.dmzadorin.demo.config.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer2;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import ru.dmzadorin.demo.messaging.PayloadMessageConsumer;
import ru.dmzadorin.demo.model.Message;
import ru.dmzadorin.demo.model.MessagesPayload;

import javax.annotation.Resource;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaPayloadConsumerConfig {

    @Value("${app.enrichedMessagesTopic}")
    private String enrichedMessagesTopic;

    @Value("${app.payloadMessagesListeners}")
    private int payloadMessagesListeners;

    @Resource(name = "commonConsumerFactoryProperties")
    private Map<String, Object> commonProperties;

    @Bean
    public PayloadMessageConsumer kafkaMessageConsumer(
            ObjectMapper mapper,
            KafkaTemplate<Long, Message> messageKafkaTemplate
    ) {
        return new PayloadMessageConsumer(mapper, enrichedMessagesTopic, messageKafkaTemplate);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<Long, MessagesPayload> payloadMessageContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<Long, MessagesPayload> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConcurrency(payloadMessagesListeners);
        factory.setConsumerFactory(consumerFactory());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setBatchListener(true);

        return factory;
    }

    @Bean
    public ConsumerFactory<Object, Object> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(payloadMessagesConsumerConfigs());
    }

    @Bean
    public Map<String, Object> payloadMessagesConsumerConfigs() {
        Map<String, Object> props = new HashMap<>(commonProperties);
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, MessagesPayload.class);
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);

        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "Payload-Message-Consumer-Group");

        return props;
    }
}
