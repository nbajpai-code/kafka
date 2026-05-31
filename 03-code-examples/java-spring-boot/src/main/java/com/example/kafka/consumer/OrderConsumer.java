package com.example.kafka.consumer;

import com.example.kafka.model.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import org.springframework.kafka.support.KafkaHeaders;

@Service
public class OrderConsumer {

    private static final Logger logger = LoggerFactory.getLogger(OrderConsumer.class);

    @KafkaListener(topics = "orders-java", groupId = "spring-billing-group")
    public void consumeOrder(
            @Payload Order order,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment) {

        logger.info("📥 Consumed Order: {} | Key (Customer): {} | Partition: {} | Offset: {}",
                order.getOrderId(), key, partition, offset);

        try {
            // Simulated Business Logic (e.g. process payment)
            if (order.getAmount() > 400.0) {
                // Simulate an exception for high value orders to trigger DLQ / Retry demo!
                throw new IllegalArgumentException("Simulated high-value transaction security exception!");
            }

            logger.info("📦 Process Success: Order {} marked as complete.", order.getOrderId());
            
            // Commit offsets synchronously back to Kafka
            acknowledgment.acknowledge();

        } catch (IllegalArgumentException ex) {
            logger.warn("⚠️ Processing error: {}. Handled by Dead Letter Topic retry policies.", ex.getMessage());
            // Throw exception so Spring's DefaultErrorHandler handles retries and routes to DLQ!
            throw ex;
        }
    }

    // Dead Letter Queue Consumer
    @KafkaListener(topics = "orders-java.DLT", groupId = "spring-dlt-billing-group")
    public void consumeDeadLetterTopic(
            @Payload Order failedOrder,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment) {

        logger.error("🚨 [DLQ INGESTION] Toxic order isolated! Order: {} from Partition {} | Offset: {}",
                failedOrder.getOrderId(), partition, offset);
        
        // Acknowledge DLQ consumption to commit offset
        acknowledgment.acknowledge();
    }
}
