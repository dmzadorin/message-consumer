package ru.dmzadorin.demo;

import org.assertj.core.api.Assertions;
import org.jetbrains.annotations.NotNull;
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

    @Value("${messages.batchSize}")
    private int batchSize;

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
        int messagesCount = batchSize * 3;
        final var messages = LongStream.range(0, messagesCount)
                .mapToObj(i -> toPlainMessage(i, "Payload " + i))
                .collect(Collectors.toList());

        payload.setMessages(messages);
        kafkaTemplate.send(payloadMessagesTopic, payload);


        await().atMost(Duration.ofSeconds(10))
                .with()
                .pollInterval(Duration.ofSeconds(1))
                .untilAsserted(() ->
                        Assertions.assertThat(messageRepository.getPlainMessages())
                                .containsExactlyInAnyOrderElementsOf(messages)
                );
        assertMessagesInDb(messages);
    }

    private void assertMessagesInDb(List<Message> expected) {
        List<Message> actualMessages = dsl.selectFrom(MESSAGE_TABLE)
                .fetch(record -> toPlainMessage(record.getMessageId(), record.getPayload()));
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

        private final List<Message> plainMessages;

        public MessageRepositoryInterceptor(MessageRepository delegate) {
            this.delegate = delegate;
            this.plainMessages = new CopyOnWriteArrayList<>();
        }

        @Override
        public void saveBatch(Collection<EnrichedMessage> messages) {
            delegate.saveBatch(messages);
            this.plainMessages.addAll(convertMessages(messages));
        }

        @Override
        public Long getPartitionOffset(int partition) {
            return delegate.getPartitionOffset(partition);
        }

        public void cleanUp() {
            plainMessages.clear();
        }

        public List<Message> getPlainMessages() {
            return plainMessages;
        }
    }

    private static List<Message> convertMessages(Collection<EnrichedMessage> enrichedMessages) {
        return enrichedMessages.stream()
                .map(m -> toPlainMessage(m.getMessageId(), m.getPayload()))
                .collect(Collectors.toList());
    }

    @NotNull
    private static Message toPlainMessage(long messageId, String payload) {
        var plainMessage = new Message();
        plainMessage.setMessageId(messageId);
        plainMessage.setPayload(payload);
        return plainMessage;
    }

    private static KafkaContainer getKafkaContainer() {
        final var container = new KafkaContainer();
        container.start();
        return container;
    }
}
