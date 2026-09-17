package com.storex.orderservice.producer;

import com.storex.orderservice.model.OrderEvent;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
public class OrderKafkaProducer {

    private final ReactiveKafkaProducerTemplate<String, OrderEvent> reactiveKafkaProducerTemplate;
    private static final String TOPIC = "storex-order-events";

    public OrderKafkaProducer(ReactiveKafkaProducerTemplate<String, OrderEvent> reactiveKafkaProducerTemplate) {
        this.reactiveKafkaProducerTemplate = reactiveKafkaProducerTemplate;
    }

    public Mono<Void> sendOrderCreatedEvent(OrderEvent orderEvent) {
        ProducerRecord<String, OrderEvent> producerRecord = new ProducerRecord<>(
                TOPIC,
                null,
                orderEvent.getOrderId(),
                orderEvent
        );
        return reactiveKafkaProducerTemplate.send(producerRecord)
                .doOnSuccess(result -> System.out.println("Sent successfully to partition: " + result.recordMetadata().partition()))
                .doOnError(throwable -> System.err.println("Error sending kafka event: " + throwable.getMessage()))
                .then();
    }
}