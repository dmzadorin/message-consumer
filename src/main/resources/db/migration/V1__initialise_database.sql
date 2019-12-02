CREATE TABLE message
(
    message_id INT       NOT NULL,
    payload    VARCHAR(100),
    timestamp  TIMESTAMP NOT NULL,
    CONSTRAINT pk_message_id PRIMARY KEY (message_id)
);