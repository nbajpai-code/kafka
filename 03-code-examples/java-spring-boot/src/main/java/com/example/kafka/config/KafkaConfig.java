package com.example.kafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConfig {

    // 1. Automatically create production-ready standard topic
    @Bean
    public NewTopic ordersTopic() {
        return TopicBuilder.name("orders-java")
                .partitions(3)
                .replicas(1) // Set to match local broker replica availability
                .build();
    }

    // 2. Automatically create associated Dead Letter Topic
    @Bean
    public NewTopic ordersDltTopic() {
        return TopicBuilder.name("orders-java.DLT")
                .partitions(1)
                .replicas(1)
                .build();
    }

    // 3. Define Retry Policies and DLQ Routing
    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<Object, Object> template) {
        // Recoverer routes un-processable toxic messages to our .DLT topic
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template);
        
        // Define retry rules: Attempt processing 3 times with a 2-second fixed backoff delay
        return new DefaultErrorHandler(recoverer, new FixedBackOff(2000L, 2));
    }
}
