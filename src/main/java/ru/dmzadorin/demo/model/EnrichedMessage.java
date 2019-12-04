package ru.dmzadorin.demo.model;

public class EnrichedMessage {
    private final Long messageId;
    private final String payload;
    private final int partition;
    private final long offset;

    public EnrichedMessage(Long messageId, String payload, int partition, long offset) {
        this.messageId = messageId;
        this.payload = payload;
        this.partition = partition;
        this.offset = offset;
    }

    public Long getMessageId() {
        return messageId;
    }

    public String getPayload() {
        return payload;
    }

    public int getPartition() {
        return partition;
    }

    public long getOffset() {
        return offset;
    }
}
