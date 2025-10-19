package com.henry.kafka.producer.demo.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
@Profile("local")
public class AutoCreateConfig {

    @Value("${spring.kafka.topic}")
    public String topic;

    @Value("${spring.kafka.topic2}")
    public String topic2;

    @Bean
    public NewTopic bookEvents(){
        return TopicBuilder.name(topic)
                .partitions(3)
                .replicas(3)
                .build();
    }

    @Bean
    public NewTopic userEvents() {
        return TopicBuilder.name(topic2)
                .partitions(3)
                .replicas(3)
                .build();
    }

}
