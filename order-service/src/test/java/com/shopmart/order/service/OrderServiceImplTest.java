package com.shopmart.order.service;

import com.shopmart.order.client.InventoryClient;
import com.shopmart.order.consumer.OrderSagaConsumer;
import com.shopmart.order.dto.OrderRequest;
import com.shopmart.order.dto.OrderResponse;
import com.shopmart.order.dto.ProductDto;
import com.shopmart.order.entity.Order;
import com.shopmart.order.entity.OrderStatus;
import com.shopmart.order.event.KafkaTopics;
import com.shopmart.order.event.OrderEvent;
import com.shopmart.order.event.SagaEventType;
import com.shopmart.order.repository.OrderRepository;
import com.shopmart.order.service.impl.OrderServiceImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.math.BigDecimal;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OrderServiceImplTest {

    @Mock
    private OrderRepository orderRepository;

    @Mock
    private InventoryClient inventoryClient;

    @Mock
    private KafkaTemplate<String, OrderEvent> kafkaTemplate;

    @InjectMocks
    private OrderServiceImpl orderService;

    @Test
    void createOrder_shouldSaveWithPendingStatus() {
        when(inventoryClient.getProductById(1L))
                .thenReturn(new ProductDto(1L, "MacBook Pro", new BigDecimal("28000000"), 10));

        when(orderRepository.save(any(Order.class))).thenAnswer(inv -> {
            Order order = inv.getArgument(0);
            order.setId(1L);
            return order;
        });

        OrderResponse result = orderService.createOrder(new OrderRequest("C001", 1L, 2));

        assertThat(result.getId()).isEqualTo(1L);
        assertThat(result.getStatus()).isEqualTo(OrderStatus.PENDING);
        assertThat(result.getTotalAmount()).isEqualTo(new BigDecimal("56000000"));

        verify(kafkaTemplate).send(eq(KafkaTopics.ORDER), eq("1"), any(OrderEvent.class));
    }

    @Test
    void cancelOrder_shouldSetCancelledWithReason() {
        Order order = Order.builder().id(1L).status(OrderStatus.PENDING).build();
        when(orderRepository.findById(1L)).thenReturn(Optional.of(order));
        when(orderRepository.save(any(Order.class))).thenAnswer(inv -> inv.getArgument(0));

        OrderResponse result = orderService.cancelOrder(1L, "Payment failed");

        assertThat(result.getStatus()).isEqualTo(OrderStatus.CANCELLED);
        assertThat(result.getFailureReason()).isEqualTo("Payment failed");
    }

    // TODO Câu 5: Viết ít nhất 1 test cho kịch bản ROLLBACK của Saga
    @Test
    void sagaRollback_whenPaymentFailed_shouldCancelOrder() {
        // Giả lập đơn hàng ban đầu đang PENDING
        Order order = Order.builder()
                .id(100L)
                .customerId("C002")
                .productId(3L)
                .quantity(3)
                .totalAmount(new BigDecimal("84000000"))
                .status(OrderStatus.PENDING)
                .build();

        when(orderRepository.findById(100L)).thenReturn(Optional.of(order));
        when(orderRepository.save(any(Order.class))).thenAnswer(inv -> inv.getArgument(0));

        // Khởi tạo Consumer xử lý sự kiện Saga
        OrderSagaConsumer consumer = new OrderSagaConsumer(orderService);

        // Nhận sự kiện PAYMENT_FAILED (do vượt hạn mức thanh toán 80.000.000 hoặc simulate failure)
        OrderEvent paymentFailedEvent = OrderEvent.builder()
                .orderId(100L)
                .productId(3L)
                .quantity(3)
                .amount(new BigDecimal("84000000"))
                .type(SagaEventType.PAYMENT_FAILED)
                .message("Số tiền vượt hạn mức 80000000")
                .build();

        consumer.handleOrderEvents(paymentFailedEvent);

        // Kiểm chứng đơn hàng đã được cập nhật thành CANCELLED với lý do cụ thể
        ArgumentCaptor<Order> orderCaptor = ArgumentCaptor.forClass(Order.class);
        verify(orderRepository).save(orderCaptor.capture());
        Order cancelledOrder = orderCaptor.getValue();

        assertThat(cancelledOrder.getStatus()).isEqualTo(OrderStatus.CANCELLED);
        assertThat(cancelledOrder.getFailureReason()).contains("80000000");
    }
}
