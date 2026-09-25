package com.shopmart.payment.consumer;

import com.shopmart.payment.dto.PaymentRequest;
import com.shopmart.payment.dto.PaymentResponse;
import com.shopmart.payment.entity.PaymentStatus;
import com.shopmart.payment.event.KafkaTopics;
import com.shopmart.payment.event.OrderEvent;
import com.shopmart.payment.event.SagaEventType;
import com.shopmart.payment.service.PaymentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Consumer xử lý thanh toán cho Saga khi nhận được sự kiện INVENTORY_RESERVED.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentSagaConsumer {

    private final PaymentService paymentService;
    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    @KafkaListener(topics = KafkaTopics.ORDER, groupId = "payment-group")
    public void handleOrderEvents(OrderEvent event) {
        log.info("[Saga-PaymentConsumer] Nhận event: type={}, orderId={}, amount={}",
                event.getType(), event.getOrderId(), event.getAmount());

        if (event.getType() == SagaEventType.INVENTORY_RESERVED) {
            log.info("[Saga Step 3] Tiến hành thanh toán cho đơn hàng orderId={}, số tiền={}",
                    event.getOrderId(), event.getAmount());
            try {
                PaymentRequest request = new PaymentRequest(event.getOrderId(), event.getAmount());
                PaymentResponse response = paymentService.processPayment(request);

                if (response.getStatus() == PaymentStatus.SUCCESS) {
                    OrderEvent successEvent = OrderEvent.builder()
                            .orderId(event.getOrderId())
                            .productId(event.getProductId())
                            .quantity(event.getQuantity())
                            .amount(event.getAmount())
                            .type(SagaEventType.PAYMENT_COMPLETED)
                            .message("Thanh toán thành công cho đơn hàng #" + event.getOrderId())
                            .build();

                    kafkaTemplate.send(KafkaTopics.ORDER, String.valueOf(event.getOrderId()), successEvent);
                    log.info("[Saga Step 3 THÀNH CÔNG] Đã publish PAYMENT_COMPLETED cho orderId={}", event.getOrderId());
                } else {
                    OrderEvent failedEvent = OrderEvent.builder()
                            .orderId(event.getOrderId())
                            .productId(event.getProductId())
                            .quantity(event.getQuantity())
                            .amount(event.getAmount())
                            .type(SagaEventType.PAYMENT_FAILED)
                            .message("Thanh toán thất bại: " + response.getMessage())
                            .build();

                    kafkaTemplate.send(KafkaTopics.ORDER, String.valueOf(event.getOrderId()), failedEvent);
                    log.error("[Saga Step 3 THẤT BẠI] Đã publish PAYMENT_FAILED cho orderId={}: {}",
                            event.getOrderId(), response.getMessage());
                }
            } catch (Exception e) {
                log.error("[Saga Step 3 NGOẠI LỆ] Lỗi xử lý thanh toán cho orderId={}: {}",
                        event.getOrderId(), e.getMessage());

                OrderEvent failedEvent = OrderEvent.builder()
                        .orderId(event.getOrderId())
                        .productId(event.getProductId())
                        .quantity(event.getQuantity())
                        .amount(event.getAmount())
                        .type(SagaEventType.PAYMENT_FAILED)
                        .message("Lỗi ngoại lệ cổng thanh toán: " + e.getMessage())
                        .build();

                kafkaTemplate.send(KafkaTopics.ORDER, String.valueOf(event.getOrderId()), failedEvent);
            }
        }
    }
}
