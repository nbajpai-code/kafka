package com.example.kafka.model;

public class Order {
    private String orderId;
    private String customerId;
    private double amount;
    private String timestamp;

    // Constructors
    public Order() {}

    public Order(String orderId, String customerId, double amount, String timestamp) {
        this.orderId = orderId;
        this.customerId = customerId;
        this.amount = amount;
        this.timestamp = timestamp;
    }

    // Getters and Setters
    public String getOrderId() { return orderId; }
    public void setOrderId(String orderId) { this.orderId = orderId; }

    public String getCustomerId() { return customerId; }
    public void setCustomerId(String customerId) { this.customerId = customerId; }

    public double getAmount() { return amount; }
    public void setAmount(double amount) { this.amount = amount; }

    public String getTimestamp() { return timestamp; }
    public void setTimestamp(String timestamp) { this.timestamp = timestamp; }

    @Override
    public String toString() {
        return "Order{" +
                "orderId='" + orderId + '\'' +
                ", customerId='" + customerId + '\'' +
                ", amount=" + amount +
                ", timestamp='" + timestamp + '\'' +
                '}';
    }
}
