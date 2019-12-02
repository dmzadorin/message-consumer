package ru.dmzadorin.demo.db;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.NonTransientDataAccessResourceException;
import ru.dmzadorin.demo.model.Message;

import java.util.Collection;

public class JooqMessageRepository implements MessagesRepository {

    private static final ru.dmzadorin.demo.db.jooq.tables.Message MESSAGE_TABLE =
            ru.dmzadorin.demo.db.jooq.tables.Message.MESSAGE;

    private static final Logger logger = LogManager.getLogger(JooqMessageRepository.class);

    private final DSLContext dsl;

    public JooqMessageRepository(DSLContext dsl) {
        this.dsl = dsl;
    }

    @Override
    public void saveOne(Message message) {
        try {
            dsl.insertInto(MESSAGE_TABLE)
                    .set(MESSAGE_TABLE.MESSAGE_ID, message.getMessageId())
                    .set(MESSAGE_TABLE.PAYLOAD, message.getPayload())
                    .set(MESSAGE_TABLE.TIMESTAMP, DSL.currentTimestamp())
                    .execute();
        } catch (NonTransientDataAccessResourceException ex) {
            logger.error("Target database is unavailable", ex);
        } catch (DataAccessException ex) {
            logger.error("Failed to save message with id '{}', cause: {}", message.getMessageId(), ex.toString());
        }
    }

    @Override
    public void saveBatch(Collection<Message> messages) {
        messages.forEach(this::saveOne);
    }
}
