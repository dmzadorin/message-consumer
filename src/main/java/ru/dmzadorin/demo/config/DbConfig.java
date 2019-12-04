package ru.dmzadorin.demo.config;

import org.jooq.ExecuteListener;
import org.jooq.ExecuteListenerProvider;
import org.jooq.impl.DataSourceConnectionProvider;
import org.jooq.impl.DefaultDSLContext;
import org.jooq.impl.DefaultExecuteListenerProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.jooq.JooqExceptionTranslator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.TransactionAwareDataSourceProxy;
import ru.dmzadorin.demo.db.MessageRepository;
import ru.dmzadorin.demo.db.JooqMessageRepository;

import javax.sql.DataSource;

@Configuration
public class DbConfig {

    @Autowired
    private DataSource dataSource;

    @Bean
    public DataSourceConnectionProvider connectionProvider() {
        return new DataSourceConnectionProvider(
                new TransactionAwareDataSourceProxy(dataSource)
        );
    }

    @Bean
    public ExecuteListenerProvider executeListenerProvider() {
        return new DefaultExecuteListenerProvider(exceptionTransformer());
    }

    @Bean
    public ExecuteListener exceptionTransformer() {
        return new JooqExceptionTranslator();
    }

    @Bean
    public MessageRepository messagesRepository(DefaultDSLContext dslContext) {
        return new JooqMessageRepository(dslContext);
    }
}
