package ru.dmzadorin.demo.model;

public class EnrichedMessage {
    private final Long messageId;
    private final String payload;
    private final long offset;

    public EnrichedMessage(Long messageId, String payload, long offset) {
        this.messageId = messageId;
        this.payload = payload;
        this.offset = offset;
    }

    public Long getMessageId() {
        return messageId;
    }

    public String getPayload() {
        return payload;
    }

    public long getOffset() {
        return offset;
    }
}
