package ru.dmzadorin.demo.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class JsonUtil {
    private static final Logger logger = LogManager.getLogger(JsonUtil.class);

    public static <T> String writeValueAsString(T dto, ObjectMapper objectMapper) {
        try {
            return objectMapper.writeValueAsString(dto);
        } catch (JsonProcessingException e) {
            logger.error("Writing value to JSON failed: ", e);
            return "";
        }
    }
}
