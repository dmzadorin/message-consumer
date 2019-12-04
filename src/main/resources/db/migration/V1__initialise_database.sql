CREATE TABLE message
(
    message_id      BIGINT    NOT NULL,
    payload         VARCHAR(100),
    timestamp       TIMESTAMP NOT NULL,
    kafka_partition INTEGER   NOT NULL,
    kafka_offset    BIGINT    NOT NULL,
    CONSTRAINT pk_message_id PRIMARY KEY (message_id)
);