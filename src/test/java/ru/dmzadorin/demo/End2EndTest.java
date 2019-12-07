package ru.dmzadorin.demo;

import org.assertj.core.api.Assertions;
import org.jooq.DSLContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import ru.dmzadorin.demo.db.JooqMessageRepository;
import ru.dmzadorin.demo.db.MessageRepository;
import ru.dmzadorin.demo.model.EnrichedMessage;
import ru.dmzadorin.demo.model.Message;
import ru.dmzadorin.demo.model.MessagesPayload;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.awaitility.Awaitility.await;

@SpringBootTest
@ActiveProfiles("e2e")
@ContextConfiguration(initializers = End2EndTest.Initializer.class)
@Testcontainers
public class End2EndTest {
    @Value("${app.payloadMessagesTopic}")
    private String payloadMessagesTopic;

    @Value("${spring.datasource.username}")
    private String dbUser;

    @Value("${spring.datasource.username}")
    private String dbPassword;

    @Autowired
    private KafkaTemplate<Long, MessagesPayload> kafkaTemplate;

    @Autowired
    private MessageRepositoryInterceptor messageRepository;

    @Autowired
    private DSLContext dsl;

    @Container
    private static KafkaContainer kafkaContainer = getKafkaContainer();

    private static final ru.dmzadorin.demo.db.jooq.tables.Message MESSAGE_TABLE =
            ru.dmzadorin.demo.db.jooq.tables.Message.MESSAGE;

    @BeforeEach
    public void setup() {
        messageRepository.cleanUp();
    }

    @Test
    void testPublishEnd2End() {
        var payload = new MessagesPayload();
        final var messages = LongStream.range(0, 3)
                .mapToObj(i -> {
                    final var message = new Message();
                    message.setMessageId(i);
                    message.setPayload("Payload " + i);
                    return message;
                }).collect(Collectors.toList());
        payload.setMessages(messages);
        kafkaTemplate.send(payloadMessagesTopic, payload);

        var expectedMessages = LongStream.range(0, 3)
                .mapToObj(i -> new EnrichedMessage(i, "Payload " + i, 0, i))
                .collect(Collectors.toList());

        await().atMost(Duration.ofSeconds(10))
                .with()
                .pollInterval(Duration.ofSeconds(1))
                .untilAsserted(() ->
                        Assertions.assertThat(messageRepository.getMessages())
                                .containsExactlyInAnyOrderElementsOf(expectedMessages)
                );
        assertMessageIds(expectedMessages);
    }

    private void assertMessageIds(List<EnrichedMessage> expected) {
        List<EnrichedMessage> actualMessages = dsl.selectFrom(MESSAGE_TABLE)
                .fetch(record ->
                        new EnrichedMessage(
                                record.getMessageId(),
                                record.getPayload(),
                                record.getKafkaPartition(),
                                record.getKafkaOffset())
                );
        Assertions.assertThat(expected).containsExactlyInAnyOrderElementsOf(actualMessages);
    }


    @TestConfiguration
    static class Ctx {
        @Bean
        public MessageRepository messageRepository(DSLContext dslContext) {
            var jooqRepo = new JooqMessageRepository(dslContext);
            return new MessageRepositoryInterceptor(jooqRepo);
        }
    }

    static class Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {

        @Override
        public void initialize(ConfigurableApplicationContext configurableApplicationContext) {
            TestPropertyValues values = TestPropertyValues.of(
                    "spring.kafka.bootstrap-servers=" + kafkaContainer.getBootstrapServers()
            );
            values.applyTo(configurableApplicationContext);
        }

    }

    static class MessageRepositoryInterceptor implements MessageRepository {

        private final MessageRepository delegate;

        private final List<EnrichedMessage> messages;

        public MessageRepositoryInterceptor(MessageRepository delegate) {
            this.delegate = delegate;
            this.messages = new CopyOnWriteArrayList<>();
        }

        @Override
        public void saveBatch(Collection<EnrichedMessage> messages) {
            delegate.saveBatch(messages);
            this.messages.addAll(messages);
        }

        @Override
        public Long getPartitionOffset(int partition) {
            return delegate.getPartitionOffset(partition);
        }

        public void cleanUp() {
            messages.clear();
        }

        public List<EnrichedMessage> getMessages() {
            return messages;
        }
    }

    private static KafkaContainer getKafkaContainer() {
        final var container = new KafkaContainer();
        container.start();
        return container;
    }
}
