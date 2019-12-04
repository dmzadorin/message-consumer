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
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.converter.BytesJsonMessageConverter;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer2;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import ru.dmzadorin.demo.messaging.KafkaMessageConsumer;
import ru.dmzadorin.demo.model.MessagesPayload;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaPayloadConsumerConfig {

    @Value("${spring.kafka.consumer.bootstrap-servers}")
    private String kafkaServer;

    @Value("${app.enrichedMessagesTopic}")
    private String enrichedMessagesTopic;

    @Autowired
    private BytesJsonMessageConverter messageConverter;

    @Bean
    public KafkaMessageConsumer kafkaMessageConsumer(
            ObjectMapper mapper,
            KafkaTemplate<Long, String> kafkaTemplate
    ) {
        return new KafkaMessageConsumer(mapper, enrichedMessagesTopic, kafkaTemplate);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<Long, MessagesPayload> payloadMessageContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<Long, MessagesPayload> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConcurrency(3);
        factory.setConsumerFactory(consumerFactory());
        factory.setMessageConverter(messageConverter);
        return factory;
    }

    @Bean
    public ConsumerFactory<Object, Object> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(singleConsumerConfigs());
    }

    @Bean
    public Map<String, Object> singleConsumerConfigs() {
        Map<String, Object> propsMap = new HashMap<>();
        propsMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        propsMap.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        propsMap.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer2.class);
        propsMap.put(ErrorHandlingDeserializer2.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
        return propsMap;
    }
}
