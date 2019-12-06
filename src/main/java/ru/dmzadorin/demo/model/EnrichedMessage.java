package ru.dmzadorin.demo.model;

import java.util.Objects;

public class EnrichedMessage {
    private final long messageId;
    private final String payload;
    private final int partition;
    private final long offset;

    public EnrichedMessage(long messageId, String payload, int partition, long offset) {
        this.messageId = messageId;
        this.payload = payload;
        this.partition = partition;
        this.offset = offset;
    }

    public long getMessageId() {
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        EnrichedMessage that = (EnrichedMessage) o;
        return partition == that.partition &&
                offset == that.offset &&
                messageId == that.messageId &&
                payload.equals(that.payload);
    }

    @Override
    public int hashCode() {
        return Objects.hash(messageId, payload, partition, offset);
    }

    @Override
    public String toString() {
        return "EnrichedMessage{" +
                "messageId=" + messageId +
                ", payload='" + payload + '\'' +
                ", partition=" + partition +
                ", offset=" + offset +
                '}';
    }
}
