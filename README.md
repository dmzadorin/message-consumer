# message-consumer
In order to start application please execute first
1. docker volume create --name postgres-data -d local (create voloum for postgresql)
2. docker-compose up -d
3. gradle build
4. After build in order to run execute java -jar message-consumer.jar 
5. The following configuration properties are used for message listener
5.1 app.payloadMessagesTopic - topic name for original messages (aka payload message)
5.2 app.payloadMessagesPartitions - amount of partitions for payloadMessagesTopic
5.3 app.payloadMessagesListeners - amount of topic listeners for payloadMessagesTopic
5.4 app.enrichedMessagesTopic - topic name for transformed messages (e.g. flattened payload message)
5.5 app.enrichedMessagesPartitions - amount of partitions for enrichedMessagesTopic
5.6 app.enrichedMessagesListeners - amount of topic listeners for enrichedMessagesTopic
5.7 messages.batchSize - amount of messages to collect before saving to database
5.8 messages.maxWaitTimeout - timeout before saving messages to db
5.9 messages.maxWaitTimeUnit - timeout unit

6. Jmeter kafka load script is defined in kafka-messages-test.jmx (need jmeter + https://github.com/GSLabDev/pepper-box)

  