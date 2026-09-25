package com.shopmart.order.consumer;

import com.shopmart.order.event.KafkaTopics;
import com.shopmart.order.event.OrderEvent;
import com.shopmart.order.event.SagaEventType;
import com.shopmart.order.service.OrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumer lắng nghe các sự kiện Saga để hoàn tất hoặc hủy (rollback) đơn hàng.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OrderSagaConsumer {

    private final OrderService orderService;

    @KafkaListener(topics = KafkaTopics.ORDER, groupId = "order-group")
    public void handleOrderEvents(OrderEvent event) {
        log.info("[Saga-OrderConsumer] Nhận event type={} cho orderId={}: {}",
                event.getType(), event.getOrderId(), event.getMessage());

        if (event.getType() == SagaEventType.PAYMENT_COMPLETED) {
            log.info("[Saga THÀNH CÔNG] Thanh toán thành công cho orderId={}. Cập nhật COMPLETED.", event.getOrderId());
            orderService.completeOrder(event.getOrderId());
        } else if (event.getType() == SagaEventType.INVENTORY_FAILED) {
            log.error("[Saga THẤT BẠI] Giữ tồn kho thất bại cho orderId={}. Cập nhật CANCELLED: {}",
                    event.getOrderId(), event.getMessage());
            orderService.cancelOrder(event.getOrderId(), "Lỗi giữ tồn kho: " + event.getMessage());
        } else if (event.getType() == SagaEventType.PAYMENT_FAILED) {
            log.error("[Saga ROLLBACK] Thanh toán thất bại cho orderId={}. Cập nhật CANCELLED: {}",
                    event.getOrderId(), event.getMessage());
            orderService.cancelOrder(event.getOrderId(), "Lỗi thanh toán: " + event.getMessage());
        }
    }
}
