package com.henry.kafka.producer.demo.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.henry.kafka.producer.demo.dto.BookEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CompletableFuture;

@Component
@Slf4j
public class UserEventProducer {

    private final KafkaTemplate<Integer, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${spring.kafka.topic2}")
    public String topic2;

    public UserEventProducer(KafkaTemplate<Integer, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public CompletableFuture<SendResult<Integer, String>> sendUserEvent(BookEvent event) throws JsonProcessingException {
        Integer key = event.bookEventId();
        String value = objectMapper.writeValueAsString(event);

        ProducerRecord<Integer, String> record = buildProducerRecord(key, value, topic2);
        return kafkaTemplate.send(record)
                .whenComplete((res, ex) -> {
                    if (ex != null) {
                        log.error("Error sending to user-events. key={}, ex={}", key, ex.getMessage(), ex);
                    } else {
                        log.info("Sent to {} | key={} | partition={}", res.getRecordMetadata().topic(), key, res.getRecordMetadata().partition());
                    }
                });
    }

    private ProducerRecord<Integer, String> buildProducerRecord(Integer key, String value, String topic) {
        List<Header> headers = List.of(new RecordHeader("event-source", "user-api".getBytes()));
        return new ProducerRecord<>(topic, null, key, value, headers);
    }
}
