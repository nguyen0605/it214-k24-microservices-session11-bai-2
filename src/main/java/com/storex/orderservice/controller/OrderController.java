package com.storex.orderservice.controller;

import com.storex.orderservice.model.OrderEvent;
import com.storex.orderservice.producer.OrderKafkaProducer;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1/orders")
public class OrderController {

    private final OrderKafkaProducer orderKafkaProducer;

    public OrderController(OrderController(OrderKafkaProducer orderKafkaProducer) {
        this.orderKafkaProducer = orderKafkaProducer;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Mono<String> createOrder(@RequestBody OrderEvent orderEvent) {
        if (orderEvent.getOrderId() == null || orderEvent.getOrderId().isEmpty()) {
            orderEvent.setOrderId(UUID.randomUUID().toString());
        }
        orderEvent.setStatus("CREATED");

        return orderKafkaProducer.sendOrderCreatedEvent(orderEvent)
                .thenReturn(orderEvent.getOrderId());
    }
}