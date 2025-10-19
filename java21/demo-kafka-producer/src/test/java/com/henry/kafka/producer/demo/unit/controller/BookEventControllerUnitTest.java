package com.henry.kafka.producer.demo.unit.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.henry.kafka.producer.demo.controller.BookEventsController;
import com.henry.kafka.producer.demo.dto.BookEvent;
import com.henry.kafka.producer.demo.producer.BookEventProducer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import com.henry.kafka.producer.demo.unit.utils.TestUtil;


import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(controllers = BookEventsController.class)
@AutoConfigureMockMvc
class BookEventControllerUnitTest {

    @Autowired
    MockMvc mockMvc;

    @Autowired
    ObjectMapper objectMapper;

    @MockitoBean
    BookEventProducer bookEventProducer;

    @Test
    void postBookEvent() throws Exception {
        BookEvent bookEvent = TestUtil.BookEventRecord(); // NEW event, valid payload
        String json = objectMapper.writeValueAsString(bookEvent);
        when(bookEventProducer.sendBookEvent_Approach2(isA(BookEvent.class))).thenReturn(null);

        mockMvc.perform(post("/v1/bookevent") // <-- correct route
                        .content(json)
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isCreated());
    }

    @Test
    void postBookEvent_4xx_validationErrors() throws Exception {
        BookEvent invalid = TestUtil.bookEventRecordWithInvalidBook(); // null id / blank name, etc.
        String json = objectMapper.writeValueAsString(invalid);
        when(bookEventProducer.sendBookEvent_Approach2(isA(BookEvent.class))).thenReturn(null);

        String expectedErrorMessage =
                "book.bookId - must not be null, book.bookName - must not be blank";

        mockMvc.perform(post("/v1/bookevent") // <-- correct route
                        .content(json)
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().is4xxClientError())
                .andExpect(content().string(expectedErrorMessage));
    }

    @Test
    void putBookEvent_ok() throws Exception {
        String json = objectMapper.writeValueAsString(TestUtil.bookEventRecordUpdate()); // UPDATE + id present
        when(bookEventProducer.sendBookEvent_Approach2(isA(BookEvent.class))).thenReturn(null);

        mockMvc.perform(put("/v1/bookevent")
                        .content(json)
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk());
    }

    @Test
    void putBookEvent_missingId_400() throws Exception {
        String json = objectMapper.writeValueAsString(TestUtil.bookEventRecordUpdateWithNullBookEventId());
        when(bookEventProducer.sendBookEvent_Approach2(isA(BookEvent.class))).thenReturn(null);

        mockMvc.perform(put("/v1/bookevent")
                        .content(json)
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().is4xxClientError())
                .andExpect(content().string("Please pass the LibraryEventId"));
    }

    @Test
    void putBookEvent_wrongType_400() throws Exception {
        // NEW but with an id → controller expects UPDATE
        String json = objectMapper.writeValueAsString(TestUtil.newBookEventRecordWithBookEventId());

        mockMvc.perform(put("/v1/bookevent")
                        .content(json)
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().is4xxClientError())
                .andExpect(content().string("Only UPDATE event type is supported"));
    }
}
