package com.example.kafka.producer;

import com.example.kafka.model.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class OrderProducer {

    private static final Logger logger = LoggerFactory.getLogger(OrderProducer.class);
    private static final String TOPIC = "orders-java";

    private final KafkaTemplate<String, Order> kafkaTemplate;

    public OrderProducer(KafkaTemplate<String, Order> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendOrder(Order order) {
        logger.info("📤 Producing Order: {} with Customer ID: {}", order.getOrderId(), order.getCustomerId());
        
        // Send message using Customer ID as message key for partition alignment
        CompletableFuture<SendResult<String, Order>> future = kafkaTemplate.send(TOPIC, order.getCustomerId(), order);

        // Modern Spring 3.x / CompletableFuture callback handling
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                logger.info("✅ Order successfully sent to partition {} with offset {}",
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                logger.error("❌ Failed to produce order: {}", ex.getMessage(), ex);
            }
        });
    }
}
