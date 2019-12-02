package ru.dmzadorin.demo.web;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.dmzadorin.demo.model.MessagesPayload;

@RestController
@RequestMapping("/web")
public class MessagesController {
    private static final Logger logger = LogManager.getLogger(MessagesController.class);

    private final String messagesTopic;
    private final ObjectMapper objectMapper;
    private final KafkaTemplate<Long, String> kafkaTemplate;

    public MessagesController(
            @Value("${messages.topic}") String messagesTopic,
            ObjectMapper objectMapper,
            KafkaTemplate<Long, String> kafkaTemplate
    ) {
        this.messagesTopic = messagesTopic;
        this.objectMapper = objectMapper;
        this.kafkaTemplate = kafkaTemplate;
    }

    @GetMapping("/status")
    public String getStatus() {
        return "ok";
    }

    @PostMapping("/saveMessage")
    public void saveMessage(@RequestBody MessagesPayload payload) {
        kafkaTemplate.send(messagesTopic, writeValueAsString(payload));
    }

    private String writeValueAsString(MessagesPayload dto) {
        try {
            return objectMapper.writeValueAsString(dto);
        } catch (JsonProcessingException e) {
            logger.error("Writing value to JSON failed: ", e);
            return "failed to convert";
        }
    }
}
