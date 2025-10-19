package com.henry.kafka.producer.demo.integration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.henry.kafka.producer.demo.dto.BookEvent;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import com.henry.kafka.producer.demo.unit.utils.TestUtil;

import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT
)
@TestPropertySource(properties = {
        // ✅ ensure @Value("${spring.kafka.topic}") and topic2 resolve
        "spring.kafka.topic=book-events",
        "spring.kafka.topic2=user-events",

        // ✅ avoid @Profile(\"local\") beans like AutoCreateConfig
        "spring.profiles.active=test",

        // ✅ harmless dummy so Kafka auto-config (if touched) has a value
        "spring.kafka.producer.bootstrap-servers=localhost:9092"
})
public class BookEventsControllerIntegrationTestApproach2 {

    @Autowired TestRestTemplate restTemplate;

    @MockitoBean KafkaTemplate<Integer, String> kafkaTemplate; // mocked
    @MockitoBean KafkaAdmin kafkaAdmin; // mocked

    @Autowired ObjectMapper objectMapper;

    // BOOK EVENTS: POST
    @Test
    void postBookEvent() throws JsonProcessingException {
        BookEvent bookEvent = TestUtil.BookEventRecord();
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<BookEvent> request = new HttpEntity<>(bookEvent, headers);

        mockProducerCall("book-events", bookEvent, objectMapper.writeValueAsString(bookEvent));

        ResponseEntity<BookEvent> response =
                restTemplate.exchange("/v1/bookevent", HttpMethod.POST, request, BookEvent.class);

        assertEquals(HttpStatus.CREATED, response.getStatusCode());
        org.mockito.Mockito.verify(kafkaTemplate, times(1)).send(any(ProducerRecord.class));
    }

    // BOOK EVENTS: PUT
    @Test
    void putBookEvent() throws JsonProcessingException {
        var bookEventUpdate = TestUtil.bookEventRecordUpdate();
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<BookEvent> request = new HttpEntity<>(bookEventUpdate, headers);

        mockProducerCall("book-events", bookEventUpdate, objectMapper.writeValueAsString(bookEventUpdate));

        ResponseEntity<BookEvent> response =
                restTemplate.exchange("/v1/bookevent", HttpMethod.PUT, request, BookEvent.class);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        org.mockito.Mockito.verify(kafkaTemplate, times(1)).send(any(ProducerRecord.class));
    }

    // USER EVENTS: POST
    @Test
    void postUserEvent() throws JsonProcessingException {
        BookEvent userEvent = TestUtil.BookEventRecord();
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<BookEvent> request = new HttpEntity<>(userEvent, headers);

        mockProducerCall("user-events", userEvent, objectMapper.writeValueAsString(userEvent));

        ResponseEntity<BookEvent> response =
                restTemplate.exchange("/v1/userevent", HttpMethod.POST, request, BookEvent.class);

        assertEquals(HttpStatus.CREATED, response.getStatusCode());
        org.mockito.Mockito.verify(kafkaTemplate, times(1)).send(any(ProducerRecord.class));
    }

    // USER EVENTS: PUT
    @Test
    void putUserEvent() throws JsonProcessingException {
        var userEventUpdate = TestUtil.bookEventRecordUpdate();
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<BookEvent> request = new HttpEntity<>(userEventUpdate, headers);

        mockProducerCall("user-events", userEventUpdate, objectMapper.writeValueAsString(userEventUpdate));

        ResponseEntity<BookEvent> response =
                restTemplate.exchange("/v1/userevent", HttpMethod.PUT, request, BookEvent.class);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        org.mockito.Mockito.verify(kafkaTemplate, times(1)).send(any(ProducerRecord.class));
    }

    // mock Kafka send
    private void mockProducerCall(String topic, BookEvent event, String payload) {
        ProducerRecord<Integer, String> rec = new ProducerRecord<>(topic, event.bookEventId(), payload);
        RecordMetadata meta = new RecordMetadata(new TopicPartition(topic, 1),
                1L, 0L, System.currentTimeMillis(), null, 0,
                payload != null ? payload.length() : 0);
        SendResult<Integer, String> sendResult = new SendResult<>(rec, meta);
        when(kafkaTemplate.send(isA(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(sendResult));
    }
}
