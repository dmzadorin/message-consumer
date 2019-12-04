package ru.dmzadorin.demo.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;
import ru.dmzadorin.demo.model.Message;
import ru.dmzadorin.demo.model.MessagesPayload;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {

    @Value("${spring.kafka.producer.bootstrap-servers}")
    private String kafkaServer;

    @Bean
    public Map<String, Object> producerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        return props;
    }

    @Bean
    public ProducerFactory<Long, Message> producerFactory() {
        final DefaultKafkaProducerFactory<Long, Message> factory =
                new DefaultKafkaProducerFactory<>(producerConfigs());
        return factory;
    }

    @Bean
    public KafkaTemplate<Long, Message> messageKafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    @Bean
    public ProducerFactory<Long, MessagesPayload> payloadFactory(){
        final DefaultKafkaProducerFactory<Long, MessagesPayload> factory =
                new DefaultKafkaProducerFactory<>(producerConfigs());
        return factory;
    }

    @Bean
    public KafkaTemplate<Long, MessagesPayload> payloadTemplate() {
        return new KafkaTemplate<>(payloadFactory());
    }
}
