package com.kafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic createTopic(){
        return TopicBuilder.name("topic1")
                .build();
    }

    @Bean
    public NewTopic createJsonTopic(){
        return TopicBuilder.name("jsonTopic1")
                .build();
    }
}
