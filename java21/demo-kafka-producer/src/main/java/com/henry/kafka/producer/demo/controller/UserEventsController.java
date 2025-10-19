package com.henry.kafka.producer.demo.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.henry.kafka.producer.demo.dto.BookEvent;
import com.henry.kafka.producer.demo.dto.BookEventType;
import com.henry.kafka.producer.demo.producer.UserEventProducer;
import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@Slf4j
public class UserEventsController {

    private final UserEventProducer userEventProducer;

    public UserEventsController(UserEventProducer userEventProducer) {
        this.userEventProducer = userEventProducer;
    }

    @PostMapping("/v1/userevent")
    public ResponseEntity<?> postUserEvent(@RequestBody @Valid BookEvent event) throws JsonProcessingException {
        if (BookEventType.NEW != event.bookEventType()) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Only NEW event type is supported");
        }
        userEventProducer.sendUserEvent(event);
        return ResponseEntity.status(HttpStatus.CREATED).body(event);
    }

    @PutMapping("/v1/userevent")
    public ResponseEntity<?> putUserEvent(@RequestBody @Valid BookEvent event) throws JsonProcessingException {
        if (event.bookEventId() == null) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Please pass the LibraryEventId");
        }
        if (BookEventType.UPDATE != event.bookEventType()) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Only UPDATE event type is supported");
        }
        userEventProducer.sendUserEvent(event);
        return ResponseEntity.ok(event);
    }
}
