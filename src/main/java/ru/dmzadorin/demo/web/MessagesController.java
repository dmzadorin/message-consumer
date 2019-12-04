package ru.dmzadorin.demo.web;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import ru.dmzadorin.demo.model.Message;
import ru.dmzadorin.demo.model.MessagesPayload;

import java.util.stream.Collectors;
import java.util.stream.LongStream;

@RestController
@RequestMapping("/web")
public class MessagesController {
    private static final Logger logger = LogManager.getLogger(MessagesController.class);

    private final String messagesTopic;
    private final KafkaTemplate<Long, MessagesPayload> kafkaTemplate;

    public MessagesController(
            @Value("${app.payloadMessagesTopic}") String messagesTopic,
            KafkaTemplate<Long, MessagesPayload> payloadTemplate
    ) {
        this.messagesTopic = messagesTopic;
        this.kafkaTemplate = payloadTemplate;
    }

    @GetMapping("/generate")
    public String generate(@RequestParam("count") int count) {
        var curr = System.currentTimeMillis();
        final var messages = LongStream.range(0, count)
                .map(i -> i + curr)
                .mapToObj(i -> {
                            var m = new Message();
                            m.setMessageId(i);
                            m.setPayload("Payload " + i);
                            return m;
                        }
                )
                .collect(Collectors.toList());
        MessagesPayload payload = new MessagesPayload();
        payload.setMessages(messages);
        logger.info("Saving {} messages to kafka topic", messages.size());
        kafkaTemplate.send(messagesTopic, payload);
        return "Sent " + messages.size() + " messages";
    }
}
