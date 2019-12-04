package ru.dmzadorin.demo.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.converter.BytesJsonMessageConverter;
import org.springframework.kafka.support.converter.RecordMessageConverter;

@Configuration
public class KafkaCommonConfig {

    @Value("${app.enrichedMessagesTopic}")
    private String enrichedMessagesTopic;

    @Value("${app.payloadMessagesTopic}")
    private String payloadMessagesTopic;

    @Bean
    public NewTopic enrichedMessagesTopic() {
        return new NewTopic(enrichedMessagesTopic, 1, (short) 1);
    }

    @Bean
    public NewTopic payloadMessagesTopic() {
        return new NewTopic(payloadMessagesTopic, 1, (short) 1);
    }

    @Bean
    public BytesJsonMessageConverter messageConverter() {
        return new BytesJsonMessageConverter();
    }
}
