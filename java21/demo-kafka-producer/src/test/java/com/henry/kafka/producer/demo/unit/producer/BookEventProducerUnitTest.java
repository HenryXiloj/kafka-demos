package com.henry.kafka.producer.demo.unit.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.henry.kafka.producer.demo.dto.BookEvent;
import com.henry.kafka.producer.demo.producer.BookEventProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;
import com.henry.kafka.producer.demo.unit.utils.TestUtil;

// If your TestUtil is elsewhere, adjust the import:

@ExtendWith(MockitoExtension.class)
class BookEventProducerUnitTest {

    private static final String TOPIC = "book-events";

    @Mock
    KafkaTemplate<Integer, String> kafkaTemplate;

    @Spy
    ObjectMapper objectMapper = new ObjectMapper();

    @InjectMocks
    BookEventProducer eventProducer;

    @BeforeEach
    void setUp() {
        // Inject the @Value field
        ReflectionTestUtils.setField(eventProducer, "topic", TOPIC);
    }

    @Test
    void sendBookEvent_Approach2_failure() throws JsonProcessingException {
        // given
        BookEvent event = TestUtil.BookEventRecord();

        // Make KafkaTemplate.send(...) complete exceptionally
        when(kafkaTemplate.send(isA(ProducerRecord.class)))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Exception Calling Kafka")));

        // when
        CompletableFuture<SendResult<Integer, String>> future = eventProducer.sendBookEvent_Approach2(event);

        // then
        ExecutionException ex = assertThrows(ExecutionException.class, future::get);
        assertEquals("Exception Calling Kafka", ex.getCause().getMessage());
    }

    @Test
    void sendBookEvent_Approach2_success() throws Exception {
        // given
        BookEvent event = TestUtil.BookEventRecord();
        String payload = objectMapper.writeValueAsString(event);

        ProducerRecord<Integer, String> producerRecord =
                new ProducerRecord<>(TOPIC, event.bookEventId(), payload);

        // fabricate metadata you expect back
        RecordMetadata metadata = new RecordMetadata(
                new TopicPartition(TOPIC, 1),
                1L,            // offset
                0L,            // leaderEpoch/unused
                System.currentTimeMillis(),
                null,          // checksum/deprecated
                0,             // serialized key size
                payload.length()
        );

        SendResult<Integer, String> sendResult = new SendResult<>(producerRecord, metadata);

        when(kafkaTemplate.send(isA(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(sendResult));

        // when
        CompletableFuture<SendResult<Integer, String>> future = eventProducer.sendBookEvent_Approach2(event);

        // then
        SendResult<Integer, String> actual = future.get();
        assertEquals(1, actual.getRecordMetadata().partition());
        assertEquals(TOPIC, actual.getRecordMetadata().topic());
    }
}
