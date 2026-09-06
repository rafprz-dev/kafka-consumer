package com.example.kafkaconsumer;

import com.example.kafkaconsumer.rest.controllers.ExampleDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class MessageConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageConsumer.class);

    @KafkaListener(topics = "${app.kafka.topic}")
    public void consume(final ExampleDto message) {
        LOGGER.info("Received Kafka message: {}", message);
    }

    @KafkaListener(topics = "example-processed")
    public void consumeProcessed(final ExampleDto message) {
        LOGGER.info("Received processed Kafka message: {}", message);
    }
}
