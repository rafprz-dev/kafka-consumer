package com.example.kafkaconsumer;

import com.example.kafkaconsumer.rest.controllers.ExampleDto;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

@Configuration
public class KafkaCustomStream {

    @Bean
    public KStream<String, ExampleDto> exampleKafkaStream(StreamsBuilder builder) {
        KStream<String, ExampleDto> stream = builder.stream(
                "messages",
                Consumed.with(new Serdes.StringSerde(), new JsonSerde<>(ExampleDto.class))
        );

        stream
                .peek((key, value) -> System.out.println("Streaming message: " + value))
                .mapValues(value -> {
                    ExampleDto processed = new ExampleDto();
                    processed.setId(value.getId());
                    processed.setMessage(value.getMessage() + " [streamed] processed");
                    processed.setCount(value.getCount() + 1);
                    processed.setDate(value.getDate());
                    return processed;
                })
                .to("example-processed", Produced.with(new Serdes.StringSerde(), new JsonSerde<>(ExampleDto.class)));

        return stream;
    }
}
