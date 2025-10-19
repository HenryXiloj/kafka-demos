package com.henry.kafka.producer.demo.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;

public record BookEvent(
        Integer bookEventId,
        BookEventType bookEventType,
        @NotNull
        @Valid
        Book book
) {
}
