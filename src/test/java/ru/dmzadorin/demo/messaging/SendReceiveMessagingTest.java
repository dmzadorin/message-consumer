package ru.dmzadorin.demo.messaging;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.autoconfigure.json.AutoConfigureJson;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import ru.dmzadorin.demo.config.ServiceConfig;
import ru.dmzadorin.demo.db.MessageRepository;
import ru.dmzadorin.demo.model.DbTemporaryUnavailable;
import ru.dmzadorin.demo.model.EnrichedMessage;
import ru.dmzadorin.demo.model.Message;
import ru.dmzadorin.demo.model.MessagesPayload;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

@SpringBootTest(classes = SendReceiveMessagingTest.Ctx.class)
@ActiveProfiles("messaging")
@AutoConfigureJson
@EnableKafka
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@EmbeddedKafka(topics = {"${app.payloadMessagesTopic}", "${app.enrichedMessagesTopic}"}, partitions = 1)
public class SendReceiveMessagingTest {

    @Configuration
    @ComponentScan(basePackages = "ru.dmzadorin.demo.config.kafka")
    @Import(ServiceConfig.class)
    static class Ctx {
    }

    @Value("${app.payloadMessagesTopic}")
    private String payloadMessagesTopic;

    @Autowired
    private EmbeddedKafkaBroker kafkaEmbedded;

    @Autowired
    private KafkaTemplate<Long, MessagesPayload> kafkaTemplate;

    @MockBean
    private MessageRepository messageRepository;

    @Autowired
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    @BeforeEach
    public void setUp() {
        // wait until the partitions are assigned
        for (MessageListenerContainer messageListenerContainer : kafkaListenerEndpointRegistry
                .getListenerContainers()) {
            ContainerTestUtils.waitForAssignment(
                    messageListenerContainer, kafkaEmbedded.getPartitionsPerTopic()
            );
        }
        when(messageRepository.getPartitionOffset(anyInt())).thenReturn(null);
    }

    @Test
    public void testSendReceive() {
        List<Message> actualMessages = configureMockMessageCapturer();

        var payload = new MessagesPayload();
        final var expectedMessages = LongStream.range(0, 3)
                .mapToObj(i -> buildMessage(i, "Payload " + i))
                .collect(Collectors.toList());
        payload.setMessages(expectedMessages);

        sendAndVerify(payload, actualMessages, expectedMessages);
    }

    @Test
    public void testMessageFiltering() {
        List<Message> actualMessages = configureMockMessageCapturer();

        var payload = new MessagesPayload();
        var messages = LongStream.range(-1, 2).mapToObj(i -> buildMessage(i, "Payload " + i));

        var messageWithNullId = new Message();
        messageWithNullId.setPayload("NULL id");
        var payloadMessages = Stream.concat(messages, Stream.ofNullable(messageWithNullId))
                .collect(Collectors.toList());
        payload.setMessages(payloadMessages);

        var expectedMessages = List.of(
                buildMessage(0L, "Payload 0"),
                buildMessage(1L, "Payload 1")
        );

        sendAndVerify(payload, actualMessages, expectedMessages);
    }

    @Test
    public void testSendBulkPayload() {
        List<Message> actualMessages = configureMockMessageCapturer();

        var payloads = new ArrayList<MessagesPayload>();
        final var payloadsCount = 5;
        final var msgPerPayload = 3;
        for (long i = 0; i < payloadsCount; i++) {
            var payloadMessages = new ArrayList<Message>();
            for (long j = 0; j < msgPerPayload; j++) {
                final var message = new Message();
                final var id = i * 10 + j;
                message.setMessageId(id);
                message.setPayload("Payload " + id);
                payloadMessages.add(message);
            }
            var payload = new MessagesPayload();
            payload.setMessages(payloadMessages);
            payloads.add(payload);
        }
        List<Message> expectedMessages = payloads.stream()
                .map(MessagesPayload::getMessages)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

        sendAndVerify(payloads, actualMessages, expectedMessages);
    }

    @Test
    public void testOffsetRewind() {
        // At first call repository would throw DbTemporaryUnavailable exception,
        // then message listener should rewind offset on partition and repository
        // would be called second time

        var actualMessages = new CopyOnWriteArrayList<Message>();
        var dbEx = new DbTemporaryUnavailable(null);
        doThrow(dbEx)
                .doAnswer(invocation -> {
                    List<EnrichedMessage> messages = invocation.getArgument(0);
                    var plainMessages = messages.stream().map(m ->
                            buildMessage(m.getMessageId(), m.getPayload())
                    ).collect(Collectors.toList());
                    actualMessages.addAll(plainMessages);
                    return null;
                }).when(messageRepository).saveBatch(anyList());

        var payload = new MessagesPayload();
        final var expectedMessages = LongStream.range(0, 3)
                .mapToObj(i -> buildMessage(i, "Payload " + i))
                .collect(Collectors.toList());
        payload.setMessages(expectedMessages);

        sendAndVerify(payload, actualMessages, expectedMessages);
    }

    private void sendAndVerify(
            MessagesPayload payload,
            List<Message> actualMessages,
            List<Message> expectedMessages
    ) {
        sendAndVerify(List.of(payload), actualMessages, expectedMessages);
    }

    private void sendAndVerify(
            List<MessagesPayload> payloads,
            List<Message> actualMessages,
            List<Message> expectedMessages
    ) {
        payloads.forEach(p -> kafkaTemplate.send(payloadMessagesTopic, p));

        await().atMost(Duration.ofSeconds(10))
                .with()
                .pollInterval(Duration.ofSeconds(1))
                .untilAsserted(() ->
                        Assertions.assertThat(actualMessages)
                        .containsExactlyInAnyOrderElementsOf(expectedMessages)
                );
    }

    private List<Message> configureMockMessageCapturer() {
        var actualMessages = new CopyOnWriteArrayList<Message>();
        doAnswer(invocation -> {
            List<EnrichedMessage> messages = invocation.getArgument(0);
            var plainMessages = messages.stream().map(m ->
                    //Need to extract just message id and poyload, since topic and
                    // partition may differ
                    buildMessage(m.getMessageId(), m.getPayload())
            ).collect(Collectors.toList());
            System.out.println("Plain messages: " + plainMessages.toString());
            actualMessages.addAll(plainMessages);
            return null;
        }).when(messageRepository).saveBatch(anyList());
        return actualMessages;
    }

    private Message buildMessage(long id, String payload) {
        final var message = new Message();
        message.setMessageId(id);
        message.setPayload(payload);
        return message;
    }
}
