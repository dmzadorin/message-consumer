package ru.dmzadorin.demo.config.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer2;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaCommonConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String kafkaServer;

    @Value("${app.enrichedMessagesTopic}")
    private String enrichedMessagesTopic;

    @Value("${app.payloadMessagesTopic}")
    private String payloadMessagesTopic;

    @Value("${app.enrichedMessagesPartitions}")
    private int enrichedMessagesPartitions;

    @Bean
    public NewTopic enrichedMessagesTopic() {
        return new NewTopic(enrichedMessagesTopic, enrichedMessagesPartitions, (short) 1);
    }

    @Bean
    public NewTopic payloadMessagesTopic() {
        return new NewTopic(payloadMessagesTopic, 1, (short) 1);
    }

    @Bean
    public Map<String, Object> commonConsumerFactoryProperties() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer2.class);
        props.put(ErrorHandlingDeserializer2.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
        return props;
    }

}
