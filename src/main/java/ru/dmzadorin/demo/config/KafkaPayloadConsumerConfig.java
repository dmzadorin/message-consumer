package ru.dmzadorin.demo.config;

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
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer2;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import ru.dmzadorin.demo.messaging.PayloadMessageConsumer;
import ru.dmzadorin.demo.model.Message;
import ru.dmzadorin.demo.model.MessagesPayload;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaPayloadConsumerConfig {

    @Value("${spring.kafka.consumer.bootstrap-servers}")
    private String kafkaServer;

    @Value("${app.enrichedMessagesTopic}")
    private String enrichedMessagesTopic;

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
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }

    @Bean
    public ConsumerFactory<Object, Object> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(payloadMessagesConsumerConfigs());
    }

    @Bean
    public Map<String, Object> payloadMessagesConsumerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer2.class);
        props.put(ErrorHandlingDeserializer2.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, MessagesPayload.class);
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);

        return props;
    }
}
