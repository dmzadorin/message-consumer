package ru.dmzadorin.demo.model;

import java.util.List;

public class MessagesPayload {
    private List<Message> messages;

    public List<Message> getMessages() {
        return messages;
    }

    public void setMessages(List<Message> messages) {
        this.messages = messages;
    }
}
