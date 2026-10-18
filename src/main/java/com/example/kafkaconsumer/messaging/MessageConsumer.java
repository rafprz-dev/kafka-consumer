package com.example.kafkaconsumer.messaging;

import com.example.kafkaconsumer.model.ExampleDto;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class MessageConsumer {

    @KafkaListener(topics = "${app.kafka.topic}")
    public void consume(final ConsumerRecord<String, ExampleDto> record) {
        log.info(
                "Received message: value={}, topic={}, partition={}, offset={}, timestamp={}, key={}, headers={}",
                record.value(),
                record.topic(),
                record.partition(),
                record.offset(),
                record.timestamp(),
                record.key(),
                record.headers()
        );
    }

    @KafkaListener(topics = "example-processed")
    public void consumeProcessed(final ConsumerRecord<String, ExampleDto> record) {
        log.info(
                "Received processed message: value={}, topic={}, partition={}, offset={}, timestamp={}, key={}, headers={}",
                record.value(),
                record.topic(),
                record.partition(),
                record.offset(),
                record.timestamp(),
                record.key(),
                record.headers()
        );
    }
}
