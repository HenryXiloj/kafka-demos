package com.henry.kafka.producer.demo.unit.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.henry.kafka.producer.demo.controller.UserEventsController;
import com.henry.kafka.producer.demo.dto.BookEvent;
import com.henry.kafka.producer.demo.producer.UserEventProducer;
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

@WebMvcTest(controllers = UserEventsController.class)
@AutoConfigureMockMvc
class UserEventsControllerUnitTest {

    @Autowired MockMvc mockMvc;
    @Autowired ObjectMapper objectMapper;

    @MockitoBean UserEventProducer userEventProducer;

    // POST /v1/userevent — happy path (NEW)
    @Test
    void postUserEvent_ok() throws Exception {
        BookEvent event = TestUtil.BookEventRecord(); // NEW + valid
        String json = objectMapper.writeValueAsString(event);
        when(userEventProducer.sendUserEvent(isA(BookEvent.class))).thenReturn(null);

        mockMvc.perform(post("/v1/userevent")
                        .content(json)
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isCreated());
    }

    // POST /v1/userevent — validation errors
    @Test
    void postUserEvent_4xx_validation() throws Exception {
        BookEvent invalid = TestUtil.bookEventRecordWithInvalidBook();
        String json = objectMapper.writeValueAsString(invalid);
        when(userEventProducer.sendUserEvent(isA(BookEvent.class))).thenReturn(null);

        String expected = "book.bookId - must not be null, book.bookName - must not be blank";

        mockMvc.perform(post("/v1/userevent")
                        .content(json)
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().is4xxClientError())
                .andExpect(content().string(expected));
    }

    // POST /v1/userevent — wrong type (expects NEW)
    @Test
    void postUserEvent_wrongType_400() throws Exception {
        BookEvent wrongType = TestUtil.bookEventRecordUpdate(); // UPDATE payload
        String json = objectMapper.writeValueAsString(wrongType);

        mockMvc.perform(post("/v1/userevent")
                        .content(json)
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isBadRequest())
                .andExpect(content().string("Only NEW event type is supported"));
    }

    // PUT /v1/userevent — happy path (UPDATE + id)
    @Test
    void putUserEvent_ok() throws Exception {
        BookEvent update = TestUtil.bookEventRecordUpdate();
        String json = objectMapper.writeValueAsString(update);
        when(userEventProducer.sendUserEvent(isA(BookEvent.class))).thenReturn(null);

        mockMvc.perform(put("/v1/userevent")
                        .content(json)
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk());
    }

    // PUT /v1/userevent — missing id
    @Test
    void putUserEvent_missingId_400() throws Exception {
        BookEvent missingId = TestUtil.bookEventRecordUpdateWithNullBookEventId();
        String json = objectMapper.writeValueAsString(missingId);
        when(userEventProducer.sendUserEvent(isA(BookEvent.class))).thenReturn(null);

        mockMvc.perform(put("/v1/userevent")
                        .content(json)
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isBadRequest())
                .andExpect(content().string("Please pass the LibraryEventId"));
    }

    // PUT /v1/userevent — wrong type (expects UPDATE)
    @Test
    void putUserEvent_wrongType_400() throws Exception {
        BookEvent newWithId = TestUtil.newBookEventRecordWithBookEventId();
        String json = objectMapper.writeValueAsString(newWithId);

        mockMvc.perform(put("/v1/userevent")
                        .content(json)
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isBadRequest())
                .andExpect(content().string("Only UPDATE event type is supported"));
    }
}
