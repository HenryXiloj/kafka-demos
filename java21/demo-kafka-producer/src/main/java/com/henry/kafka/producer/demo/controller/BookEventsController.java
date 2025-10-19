package com.henry.kafka.producer.demo.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.henry.kafka.producer.demo.dto.BookEvent;
import com.henry.kafka.producer.demo.dto.BookEventType;
import com.henry.kafka.producer.demo.producer.BookEventProducer;
import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
public class BookEventsController {

    private final BookEventProducer bookEventProducer;

    public BookEventsController(BookEventProducer bookEventProducer) {
        this.bookEventProducer = bookEventProducer;
    }

    @PostMapping("/v1/bookevent")
    public ResponseEntity<?> postBookEvent(@RequestBody @Valid BookEvent bookEvent) throws JsonProcessingException {

        if (BookEventType.NEW != bookEvent.bookEventType()) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Only NEW event type is supported");
        }
        //invoke kafka producer
        bookEventProducer.sendBookEvent_Approach2(bookEvent);
        return ResponseEntity.status(HttpStatus.CREATED).body(bookEvent);
    }

    //PUT
    @PutMapping("/v1/bookevent")
    public ResponseEntity<?> putBookEvent(@RequestBody @Valid BookEvent bookEvent) throws JsonProcessingException {


        ResponseEntity<String> BAD_REQUEST = validateLibraryEvent(bookEvent);
        if (BAD_REQUEST != null) return BAD_REQUEST;

        bookEventProducer.sendBookEvent_Approach2(bookEvent);
        log.info("after produce call");
        return ResponseEntity.status(HttpStatus.OK).body(bookEvent);
    }

    private static ResponseEntity<String> validateLibraryEvent(BookEvent libraryEvent) {
        if (libraryEvent.bookEventId() == null) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Please pass the LibraryEventId");
        }

        if (!BookEventType.UPDATE.equals(libraryEvent.bookEventType())) {
            log.info("Inside the if block");
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Only UPDATE event type is supported");
        }
        return null;
    }


}
