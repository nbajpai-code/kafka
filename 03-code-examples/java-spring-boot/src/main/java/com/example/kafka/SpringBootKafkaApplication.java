package com.example.kafka;

import com.example.kafka.model.Order;
import com.example.kafka.producer.OrderProducer;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.time.Instant;
import java.util.UUID;

@SpringBootApplication
public class SpringBootKafkaApplication {

    public static void main(String[] args) {
        SpringApplication.run(SpringBootKafkaApplication.class, args);
    }

    @Bean
    public CommandLineRunner demoProducer(OrderProducer producer) {
        return args -> {
            System.out.println("⏳ Waiting 3 seconds for cluster connection...");
            Thread.sleep(3000);
            
            System.out.println("🚀 Triggering Spring Boot Demo Producer...");
            for (int i = 1; i <= 5; i++) {
                Order order = new Order(
                        UUID.randomUUID().toString(),
                        "CUST-" + (100 + i),
                        99.99 * i,
                        Instant.now().toString()
                );
                producer.sendOrder(order);
                Thread.sleep(1000);
            }
            System.out.println("✅ Spring Boot Demo Producer finished sending sample batch.");
        };
    }
}
