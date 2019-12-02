package ru.dmzadorin.demo.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.kafka.support.converter.BytesJsonMessageConverter;
import ru.dmzadorin.demo.db.MessagesRepository;
import ru.dmzadorin.demo.messaging.KafkaMessageConsumer;

@Configuration
public class KafkaConsumerConfig {

    @Bean
    public BytesJsonMessageConverter messageConverter() {
        return new BytesJsonMessageConverter();
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> kafkaConsumerFactory
    ) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, kafkaConsumerFactory);
        factory.setErrorHandler(new SeekToCurrentErrorHandler()); // <<<<<<
        return factory;
    }

    @Bean
    public KafkaMessageConsumer kafkaMessageConsumer(ObjectMapper mapper, MessagesRepository messagesRepository) {
        return new KafkaMessageConsumer(mapper, messagesRepository);
    }
}
