package ru.dmzadorin.demo.web;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import ru.dmzadorin.demo.model.Message;
import ru.dmzadorin.demo.model.MessagesPayload;
import ru.dmzadorin.demo.util.JsonUtil;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

@RestController
@RequestMapping("/web")
public class MessagesController {
    private static final Logger logger = LogManager.getLogger(MessagesController.class);

    private final String messagesTopic;
    private final ObjectMapper objectMapper;
    private final KafkaTemplate<Long, String> kafkaTemplate;

    public MessagesController(
            @Value("${app.payloadMessagesTopic}") String messagesTopic,
            ObjectMapper objectMapper,
            KafkaTemplate<Long, String> kafkaTemplate
    ) {
        this.messagesTopic = messagesTopic;
        this.objectMapper = objectMapper;
        this.kafkaTemplate = kafkaTemplate;
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
        kafkaTemplate.send(messagesTopic, JsonUtil.writeValueAsString(payload, objectMapper));
        return "Sent " + messages.size() + " messages";
    }

    @PostMapping(value = "/saveMessage", consumes = MediaType.APPLICATION_JSON_VALUE)
    public void saveMessage(@RequestBody MessagesPayload payload) {
        kafkaTemplate.send(messagesTopic, JsonUtil.writeValueAsString(payload, objectMapper));
    }
}
