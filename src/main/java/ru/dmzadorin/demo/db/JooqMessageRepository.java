package ru.dmzadorin.demo.db;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.InsertSetMoreStep;
import org.jooq.impl.DSL;
import org.postgresql.translation.messages_bg;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.NonTransientDataAccessResourceException;
import ru.dmzadorin.demo.db.jooq.tables.Message;
import ru.dmzadorin.demo.db.jooq.tables.records.MessageRecord;
import ru.dmzadorin.demo.model.EnrichedMessage;

import java.util.Collection;
import java.util.stream.Collectors;

public class JooqMessageRepository implements MessageRepository {

    private static final Logger logger = LogManager.getLogger(JooqMessageRepository.class);

    private static final Message MESSAGE_TABLE = Message.MESSAGE;

    private final DSLContext dsl;

    public JooqMessageRepository(DSLContext dsl) {
        this.dsl = dsl;
    }

    @Override
    public void saveBatch(Collection<EnrichedMessage> messages) {
        logger.debug("Got {} messages in batch", messages.size());
        try {
            dsl.batch(
                    messages.stream().map(this::prepareInsert).collect(Collectors.toList())
            ).execute();
            logger.debug("Batch with size {} successfully saved", messages.size());
        } catch (NonTransientDataAccessResourceException ex) {
            logger.error("Target database is unavailable", ex);
        } catch (DataAccessException ex) {
            logger.error("Failed to save messages batch, cause: {}", ex.toString());
        }
    }

    @Override
    public Long getKafkaOffset() {
        try {
            return dsl.select(DSL.max(MESSAGE_TABLE.KAFKA_OFFSET).as("maxOffset"))
                    .from(MESSAGE_TABLE).fetchOne("maxOffset", Long.class);
        } catch (RuntimeException ex) {
            logger.error("Failed to get kafka offset, cause: {}", ex.toString());
            return null;
        }
    }

    private InsertSetMoreStep<MessageRecord> prepareInsert(EnrichedMessage message) {
        return dsl.insertInto(MESSAGE_TABLE)
                .set(MESSAGE_TABLE.MESSAGE_ID, message.getMessageId())
                .set(MESSAGE_TABLE.PAYLOAD, message.getPayload())
                .set(MESSAGE_TABLE.TIMESTAMP, DSL.currentTimestamp())
                .set(MESSAGE_TABLE.KAFKA_OFFSET, message.getOffset())
                ;
    }
}
