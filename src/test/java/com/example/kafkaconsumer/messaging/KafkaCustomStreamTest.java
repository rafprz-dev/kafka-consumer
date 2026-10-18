package com.example.kafkaconsumer.messaging;

import com.example.kafkaconsumer.model.ExampleDto;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.time.LocalDate;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaCustomStreamTest {

    private static final String INPUT_TOPIC = "messages";
    private static final String OUTPUT_TOPIC = "example-processed";

    @Test
    void forwardsTransformedMessageToProcessedTopic() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, ExampleDto> stream = new KafkaCustomStream().exampleKafkaStream(builder);
        final JsonSerde<ExampleDto> exampleDtoSerde = new JsonSerde<>(ExampleDto.class);

        assertThat(stream).isNotNull();

        try (TopologyTestDriver driver = new TopologyTestDriver(builder.build(), streamProperties())) {
            final TestInputTopic<String, ExampleDto> inputTopic = driver.createInputTopic(
                    INPUT_TOPIC,
                    Serdes.String().serializer(),
                    exampleDtoSerde.serializer()
            );
            final TestOutputTopic<String, ExampleDto> outputTopic = driver.createOutputTopic(
                    OUTPUT_TOPIC,
                    Serdes.String().deserializer(),
                    exampleDtoSerde.deserializer()
            );
            final ExampleDto input = exampleDto();

            inputTopic.pipeInput("event-17", input);

            assertThat(outputTopic.readKeyValue())
                    .satisfies(record -> {
                        assertThat(record.key).isEqualTo("event-17");
                        assertThat(record.value)
                                .usingRecursiveComparison()
                                .isEqualTo(expectedProcessedExampleDto());
                    });
            assertThat(outputTopic.isEmpty()).isTrue();
        }
    }

    private Properties streamProperties() {
        final Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-custom-stream-test");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        return properties;
    }

    private ExampleDto exampleDto() {
        final ExampleDto message = new ExampleDto();
        message.setId(17L);
        message.setMessage("inventory updated");
        message.setCount(3);
        message.setDate(LocalDate.of(2026, 10, 18));
        return message;
    }

    private ExampleDto expectedProcessedExampleDto() {
        final ExampleDto message = exampleDto();
        message.setMessage("inventory updated [streamed] processed");
        message.setCount(4);
        return message;
    }
}
