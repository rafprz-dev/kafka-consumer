package com.example.kafkaconsumer.rest.controllers;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ExampleController {

    private final KafkaTemplate<String, ExampleDto> kafkaTemplate;
    private final String topic;

    public ExampleController(KafkaTemplate<String, ExampleDto> kafkaTemplate,
                            @Value("${app.kafka.topic}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    @GetMapping("/example")
    public ExampleDto getExample() {
        ExampleDto dto = new ExampleDto();
        dto.setId(1L);
        dto.setMessage("Example message");
        dto.setCount(3);
        dto.setDate(java.time.LocalDate.now());

        kafkaTemplate.send(topic, dto);
        return dto;
    }

    @GetMapping("/example2")
    public ExampleDto getExample2() {
        ExampleDto dto = new ExampleDto();
        dto.setId(2L);
        dto.setMessage("Example message from KafkaCustomStream");
        dto.setCount(5);
        dto.setDate(java.time.LocalDate.now());

        kafkaTemplate.send("example-processed", dto);
        return dto;
    }
}
