package com.shopmart.inventory.consumer;

import com.shopmart.inventory.event.KafkaTopics;
import com.shopmart.inventory.event.OrderEvent;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Consumer xử lý sự kiện Kafka theo phong cách Reactive (WebFlux / Reactor-Kafka).
 * Minh chứng tư duy Reactive, phi đồng bộ (Async / Event-driven), Backpressure và Non-blocking.
 */
@Slf4j
@Component
public class ReactiveInventoryEventConsumer {

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    private Disposable subscription;

    @PostConstruct
    public void start() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "inventory-reactive-audit-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, OrderEvent.class.getName());

        ReceiverOptions<String, OrderEvent> receiverOptions = ReceiverOptions.<String, OrderEvent>create(props)
                .subscription(Collections.singleton(KafkaTopics.ORDER));

        KafkaReceiver<String, OrderEvent> receiver = KafkaReceiver.create(receiverOptions);

        log.info("[Reactive WebFlux] Khởi động Reactive Kafka Receiver lắng nghe topic '{}'...", KafkaTopics.ORDER);

        subscription = receiver.receive()
                .doOnNext(this::processRecord)
                .doOnError(err -> log.error("[Reactive WebFlux] Lỗi trong stream nhận Kafka: {}", err.getMessage()))
                .subscribe();
    }

    private void processRecord(ReceiverRecord<String, OrderEvent> record) {
        OrderEvent event = record.value();
        if (event != null) {
            log.info("[Reactive WebFlux Consumer] Đã nhận stream OrderEvent: type={}, orderId={}, message={}",
                    event.getType(), event.getOrderId(), event.getMessage());
        }
        record.receiverOffset().acknowledge();
    }

    @PreDestroy
    public void stop() {
        if (subscription != null && !subscription.isDisposed()) {
            subscription.dispose();
            log.info("[Reactive WebFlux] Đã dừng Reactive Kafka Receiver.");
        }
    }
}
