package ru.dmzadorin.demo.db;

import org.jooq.DSLContext;
import org.jooq.conf.RenderNameStyle;
import org.jooq.conf.Settings;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.jdbc.EmbeddedDatabaseConnection;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.jooq.JooqTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;
import ru.dmzadorin.demo.config.DbConfig;
import ru.dmzadorin.demo.db.jooq.tables.Message;
import ru.dmzadorin.demo.model.EnrichedMessage;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

@Import(DbConfig.class)
@JooqTest
@ActiveProfiles("db")
@AutoConfigureTestDatabase(connection = EmbeddedDatabaseConnection.H2)
public class MessageRepositoryTest {

    private static final Message MESSAGE_TABLE = Message.MESSAGE;

    @Autowired
    private MessageRepository messageRepository;

    @Autowired
    private DSLContext dsl;

    @BeforeEach
    public void setup() {
        dsl.deleteFrom(MESSAGE_TABLE).execute();
    }

    @Test
    public void testBatchSaving() {
        var msgCount = 10;
        var messages = LongStream.range(0, msgCount)
                .mapToObj(i -> new EnrichedMessage(i, "Payload " + i, 0, i))
                .collect(Collectors.toList());
        messageRepository.saveBatch(messages);
        int count = dsl.selectCount().from(MESSAGE_TABLE).fetchOne(0, int.class);
        Assertions.assertEquals(msgCount, count);
    }

    @Test
    public void testSavingDuplicatesDoesNotPropagateException() {
        var messages = List.of(
                new EnrichedMessage(0L, "Payload 0", 0, 0),
                new EnrichedMessage(1L, "Payload 1", 0, 1),
                new EnrichedMessage(0L, "Payload 0", 0, 0)
        );

        messageRepository.saveBatch(messages);
        assertMessageIds(List.of(0L, 1L));

        messageRepository.saveBatch(List.of(
                new EnrichedMessage(2L, "Payload 2", 0, 2),
                new EnrichedMessage(3L, "Payload 3", 0, 3),
                new EnrichedMessage(1L, "Payload 1", 0, 1)
        ));

        assertMessageIds(List.of(0L, 1L, 2L, 3L));
    }

    private void assertMessageIds(List<Long> expected) {
        List<Long> actualMessageIds = dsl.selectFrom(MESSAGE_TABLE).fetch(MESSAGE_TABLE.MESSAGE_ID);
        Assertions.assertEquals(expected, actualMessageIds);
    }

    @TestConfiguration
    static class Ctx {

        @Bean
        Settings jooqSettings() {
            //Need to set render name style for jooq in order not to quote identifiers
            //Without that when querying H2 jooq fails to resolve schema/table
            return new Settings().withRenderNameStyle(RenderNameStyle.AS_IS);
        }
    }
}
