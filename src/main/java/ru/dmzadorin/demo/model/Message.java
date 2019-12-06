package ru.dmzadorin.demo.model;

import java.util.Objects;

public class Message {
    private Long messageId;
    private String payload;

    public Long getMessageId() {
        return messageId;
    }

    public void setMessageId(Long messageId) {
        this.messageId = messageId;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Message message = (Message) o;
        return Objects.equals(messageId, message.messageId) &&
                Objects.equals(payload, message.payload);
    }

    @Override
    public int hashCode() {
        return Objects.hash(messageId, payload);
    }

    @Override
    public String toString() {
        return "Message{" +
                "messageId=" + messageId +
                ", payload='" + payload + '\'' +
                '}';
    }
}
