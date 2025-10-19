package com.henry.kafka.producer.demo.unit.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.henry.kafka.producer.demo.dto.Book;
import com.henry.kafka.producer.demo.dto.BookEvent;
import com.henry.kafka.producer.demo.dto.BookEventType;

public class TestUtil {

    private static final ObjectMapper mapper = new ObjectMapper();

    // ---- Book mocks ----
    public static Book bookRecord() {
        return new Book(123, "Test", "Kafka Using Spring Boot");
    }

    public static Book bookRecordWithInvalidValues() {
        return new Book(null, "", "Kafka Using Spring Boot");
    }

    // ---- BookEvent mocks ----
    public static BookEvent BookEventRecord() {
        return new BookEvent(null, BookEventType.NEW, bookRecord());
    }

    public static BookEvent newBookEventRecordWithBookEventId() {
        return new BookEvent(123, BookEventType.NEW, bookRecord());
    }

    public static BookEvent bookEventRecordUpdate() {
        return new BookEvent(123, BookEventType.UPDATE, bookRecord());
    }

    public static BookEvent bookEventRecordUpdateWithNullBookEventId() {
        return new BookEvent(null, BookEventType.UPDATE, bookRecord());
    }

    public static BookEvent bookEventRecordWithInvalidBook() {
        return new BookEvent(null, BookEventType.NEW, bookRecordWithInvalidValues());
    }

    // ---- Utility to parse BookEvent JSON ----
    public static BookEvent parseBookEventRecord(ObjectMapper objectMapper, String json) {
        try {
            return objectMapper.readValue(json, BookEvent.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    // ---- Optional convenience overload ----
    public static BookEvent parseBookEventRecord(String json) {
        return parseBookEventRecord(mapper, json);
    }
}
