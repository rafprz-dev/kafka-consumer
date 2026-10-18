package com.example.kafkaconsumer.api;

import com.example.kafkaconsumer.model.ExampleDto;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.test.web.servlet.MockMvc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.standaloneSetup;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class ExampleControllerTest {

    private MockMvc mockMvc;
    private RecordingKafkaTemplate kafkaTemplate;

    @BeforeEach
    void setUp() {
        kafkaTemplate = new RecordingKafkaTemplate();
        mockMvc = standaloneSetup(new ExampleController(kafkaTemplate, "messages"))
                .setMessageConverters(new MappingJackson2HttpMessageConverter(objectMapper()))
                .build();
    }

    @Test
    void getExampleReturnsAndPublishesExampleMessage() throws Exception {
        mockMvc.perform(get("/example"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(1))
                .andExpect(jsonPath("$.message").value("Example message"))
                .andExpect(jsonPath("$.count").value(3))
                .andExpect(jsonPath("$.date").isString());

        assertThat(kafkaTemplate.topic()).isEqualTo("messages");
        final ExampleDto publishedMessage = kafkaTemplate.message();

        assertThat(publishedMessage)
                .extracting(ExampleDto::getId, ExampleDto::getMessage, ExampleDto::getCount)
                .containsExactly(1L, "Example message", 3);
        assertThat(publishedMessage.getDate()).isNotNull();
    }

    @Test
    void getExample2ReturnsAndPublishesProcessedMessage() throws Exception {
        mockMvc.perform(get("/example2"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(2))
                .andExpect(jsonPath("$.message").value("Example message from KafkaCustomStream"))
                .andExpect(jsonPath("$.count").value(5))
                .andExpect(jsonPath("$.date").isString());

        assertThat(kafkaTemplate.topic()).isEqualTo("example-processed");
        final ExampleDto publishedMessage = kafkaTemplate.message();

        assertThat(publishedMessage)
                .extracting(ExampleDto::getId, ExampleDto::getMessage, ExampleDto::getCount)
                .containsExactly(2L, "Example message from KafkaCustomStream", 5);
        assertThat(publishedMessage.getDate()).isNotNull();
    }

    private static final class RecordingKafkaTemplate extends KafkaTemplate<String, ExampleDto> {

        private String topic;
        private ExampleDto message;

        private RecordingKafkaTemplate() {
            super(new DefaultKafkaProducerFactory<>(java.util.Map.of(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "unused:9092"
            )));
        }

        @Override
        public java.util.concurrent.CompletableFuture<SendResult<String, ExampleDto>> send(
                final String topic,
                final ExampleDto message
        ) {
            this.topic = topic;
            this.message = message;
            return java.util.concurrent.CompletableFuture.completedFuture(null);
        }

        private String topic() {
            return topic;
        }

        private ExampleDto message() {
            return message;
        }
    }

    private ObjectMapper objectMapper() {
        return new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }
}
