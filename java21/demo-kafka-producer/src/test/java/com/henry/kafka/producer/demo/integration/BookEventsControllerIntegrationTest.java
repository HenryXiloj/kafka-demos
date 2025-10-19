package com.henry.kafka.producer.demo.integration;

import com.henry.kafka.producer.demo.unit.utils.TestUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.henry.kafka.producer.demo.dto.BookEvent;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT
)
@EmbeddedKafka(topics = {"book-events", "user-events"}, partitions = 3)
@TestPropertySource(properties = {
        "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.admin.properties.bootstrap.servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.topic=book-events",
        "spring.kafka.topic2=user-events"
})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class BookEventsControllerIntegrationTest {

    @Autowired
    TestRestTemplate restTemplate;

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    ObjectMapper objectMapper;

    private Consumer<Integer, String> consumer;

    @BeforeEach
    void setUp() {
        Map<String, Object> configs =
                new HashMap<>(KafkaTestUtils.consumerProps("group1", "true", embeddedKafkaBroker));
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        consumer = new DefaultKafkaConsumerFactory<>(
                configs, new IntegerDeserializer(), new StringDeserializer()
        ).createConsumer();
        embeddedKafkaBroker.consumeFromAllEmbeddedTopics(consumer);
    }

    @AfterEach
    void tearDown() {
        consumer.close();
    }

    // ✅ Book Events POST
    @Test
    void postBookEvent() throws JsonProcessingException {
        BookEvent bookEvent = TestUtil.BookEventRecord();
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<BookEvent> request = new HttpEntity<>(bookEvent, headers);

        ResponseEntity<BookEvent> response =
                restTemplate.exchange("/v1/bookevent", HttpMethod.POST, request, BookEvent.class);

        assertEquals(HttpStatus.CREATED, response.getStatusCode());

        ConsumerRecords<Integer, String> records = KafkaTestUtils.getRecords(consumer);
        assertEquals(1, records.count());
        records.forEach(r -> {
            var actual = TestUtil.parseBookEventRecord(objectMapper, r.value());
            assertEquals(bookEvent, actual);
        });
    }

    // ✅ Book Events PUT
    @Test
    void putBookEvent() throws JsonProcessingException {
        var bookEventUpdate = TestUtil.bookEventRecordUpdate();
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<BookEvent> request = new HttpEntity<>(bookEventUpdate, headers);

        ResponseEntity<BookEvent> response =
                restTemplate.exchange("/v1/bookevent", HttpMethod.PUT, request, BookEvent.class);

        assertEquals(HttpStatus.OK, response.getStatusCode());

        ConsumerRecords<Integer, String> records = KafkaTestUtils.getRecords(consumer);
        assertEquals(1, records.count());
        records.forEach(r -> {
            var actual = TestUtil.parseBookEventRecord(objectMapper, r.value());
            assertEquals(bookEventUpdate, actual);
        });
    }

    // ✅ User Events POST
    @Test
    void postUserEvent() throws JsonProcessingException {
        BookEvent userEvent = TestUtil.BookEventRecord();
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<BookEvent> request = new HttpEntity<>(userEvent, headers);

        ResponseEntity<BookEvent> response =
                restTemplate.exchange("/v1/userevent", HttpMethod.POST, request, BookEvent.class);

        assertEquals(HttpStatus.CREATED, response.getStatusCode());

        ConsumerRecords<Integer, String> records = KafkaTestUtils.getRecords(consumer);
        assertEquals(1, records.count());
        records.forEach(r -> {
            var actual = TestUtil.parseBookEventRecord(objectMapper, r.value());
            assertEquals(userEvent, actual);
        });
    }

    // ✅ User Events PUT
    @Test
    void putUserEvent() throws JsonProcessingException {
        var userEventUpdate = TestUtil.bookEventRecordUpdate();
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<BookEvent> request = new HttpEntity<>(userEventUpdate, headers);

        ResponseEntity<BookEvent> response =
                restTemplate.exchange("/v1/userevent", HttpMethod.PUT, request, BookEvent.class);

        assertEquals(HttpStatus.OK, response.getStatusCode());

        ConsumerRecords<Integer, String> records = KafkaTestUtils.getRecords(consumer);
        assertEquals(1, records.count());
        records.forEach(r -> {
            var actual = TestUtil.parseBookEventRecord(objectMapper, r.value());
            assertEquals(userEventUpdate, actual);
        });
    }
}
