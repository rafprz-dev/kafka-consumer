package com.example.kafkaconsumer.messaging;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.example.kafkaconsumer.model.ExampleDto;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;

import static org.assertj.core.api.Assertions.assertThat;

class MessageConsumerTest {

    private final MessageConsumer messageConsumer = new MessageConsumer();

    @Test
    void consumeLogsReceivedMessageWithRecordMetadata() {
        final ExampleDto message = exampleDto();
        final ConsumerRecord<String, ExampleDto> record =
                new ConsumerRecord<>("example-events", 2, 17L, "event-17", message);

        final ILoggingEvent logEvent = captureLog(() -> messageConsumer.consume(record));

        assertThat(logEvent.getLevel()).isEqualTo(Level.INFO);
        assertThat(logEvent.getFormattedMessage()).contains(
                "Received message:",
                "value=" + message,
                "topic=example-events",
                "partition=2",
                "offset=17",
                "timestamp=-1",
                "key=event-17",
                "headers=RecordHeaders("
        );
    }

    @Test
    void consumeProcessedLogsReceivedProcessedMessageWithRecordMetadata() {
        final ExampleDto message = exampleDto();
        final ConsumerRecord<String, ExampleDto> record =
                new ConsumerRecord<>("example-processed", 0, 18L, "event-18", message);

        final ILoggingEvent logEvent = captureLog(() -> messageConsumer.consumeProcessed(record));

        assertThat(logEvent.getLevel()).isEqualTo(Level.INFO);
        assertThat(logEvent.getFormattedMessage()).contains(
                "Received processed message:",
                "value=" + message,
                "topic=example-processed",
                "partition=0",
                "offset=18",
                "timestamp=-1",
                "key=event-18",
                "headers=RecordHeaders("
        );
    }

    private ILoggingEvent captureLog(final Runnable listenerInvocation) {
        final Logger logger = (Logger) LoggerFactory.getLogger(MessageConsumer.class);
        final ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);

        try {
            listenerInvocation.run();
            assertThat(appender.list).hasSize(1);
            return appender.list.getFirst();
        } finally {
            logger.detachAppender(appender);
            appender.stop();
        }
    }

    private ExampleDto exampleDto() {
        final ExampleDto message = new ExampleDto();
        message.setId(17L);
        message.setMessage("inventory updated");
        message.setCount(3);
        message.setDate(LocalDate.of(2026, 10, 18));
        return message;
    }
}
