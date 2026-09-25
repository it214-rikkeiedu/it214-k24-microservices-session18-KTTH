package com.shopmart.inventory.consumer;

import com.shopmart.inventory.event.KafkaTopics;
import com.shopmart.inventory.event.OrderEvent;
import com.shopmart.inventory.event.SagaEventType;
import com.shopmart.inventory.service.ProductService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Consumer lắng nghe sự kiện Saga cho inventory-service:
 * - ORDER_CREATED: Giữ tồn kho (decreaseStock)
 * - PAYMENT_FAILED: Hoàn tác tồn kho (compensating transaction - increaseStock)
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class InventorySagaConsumer {

    private final ProductService productService;
    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    @KafkaListener(topics = KafkaTopics.ORDER, groupId = "inventory-group")
    public void handleOrderEvents(OrderEvent event) {
        log.info("[Saga-InventoryConsumer] Nhận event: type={}, orderId={}, productId={}, quantity={}",
                event.getType(), event.getOrderId(), event.getProductId(), event.getQuantity());

        if (event.getType() == SagaEventType.ORDER_CREATED) {
            log.info("[Saga Step 2] Tiến hành trừ tồn kho cho orderId={}, productId={}, quantity={}",
                    event.getOrderId(), event.getProductId(), event.getQuantity());
            try {
                productService.decreaseStock(event.getProductId(), event.getQuantity());

                OrderEvent reservedEvent = OrderEvent.builder()
                        .orderId(event.getOrderId())
                        .productId(event.getProductId())
                        .quantity(event.getQuantity())
                        .amount(event.getAmount())
                        .type(SagaEventType.INVENTORY_RESERVED)
                        .message("Trừ tồn kho thành công cho đơn hàng #" + event.getOrderId())
                        .build();

                kafkaTemplate.send(KafkaTopics.ORDER, String.valueOf(event.getOrderId()), reservedEvent);
                log.info("[Saga Step 2 THÀNH CÔNG] Đã publish INVENTORY_RESERVED cho orderId={}", event.getOrderId());
            } catch (Exception e) {
                log.error("[Saga Step 2 THẤT BẠI] Không thể trừ tồn kho cho orderId={}: {}",
                        event.getOrderId(), e.getMessage());

                OrderEvent failedEvent = OrderEvent.builder()
                        .orderId(event.getOrderId())
                        .productId(event.getProductId())
                        .quantity(event.getQuantity())
                        .amount(event.getAmount())
                        .type(SagaEventType.INVENTORY_FAILED)
                        .message("Lỗi tồn kho: " + e.getMessage())
                        .build();

                kafkaTemplate.send(KafkaTopics.ORDER, String.valueOf(event.getOrderId()), failedEvent);
            }
        } else if (event.getType() == SagaEventType.PAYMENT_FAILED) {
            log.warn("[Saga COMPENSATING] Nhận PAYMENT_FAILED cho orderId={}. Tiến hành hoàn tồn kho: productId={}, quantity={}",
                    event.getOrderId(), event.getProductId(), event.getQuantity());
            try {
                productService.increaseStock(event.getProductId(), event.getQuantity());

                OrderEvent releasedEvent = OrderEvent.builder()
                        .orderId(event.getOrderId())
                        .productId(event.getProductId())
                        .quantity(event.getQuantity())
                        .amount(event.getAmount())
                        .type(SagaEventType.INVENTORY_RELEASED)
                        .message("Đã hoàn lại tồn kho thành công (Rollback)")
                        .build();

                kafkaTemplate.send(KafkaTopics.ORDER, String.valueOf(event.getOrderId()), releasedEvent);
                log.info("[Saga COMPENSATING THÀNH CÔNG] Đã hoàn tồn kho cho orderId={}", event.getOrderId());
            } catch (Exception e) {
                log.error("[Saga COMPENSATING THẤT BẠI] Lỗi hoàn tồn kho cho orderId={}: {}",
                        event.getOrderId(), e.getMessage());
            }
        }
    }
}
