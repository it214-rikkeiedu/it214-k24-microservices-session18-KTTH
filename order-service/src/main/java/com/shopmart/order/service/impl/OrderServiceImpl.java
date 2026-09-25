package com.shopmart.order.service.impl;

import com.shopmart.order.client.InventoryClient;
import com.shopmart.order.dto.OrderRequest;
import com.shopmart.order.dto.OrderResponse;
import com.shopmart.order.dto.ProductDto;
import com.shopmart.order.entity.Order;
import com.shopmart.order.entity.OrderStatus;
import com.shopmart.order.event.KafkaTopics;
import com.shopmart.order.event.OrderEvent;
import com.shopmart.order.event.SagaEventType;
import com.shopmart.order.exception.ResourceNotFoundException;
import com.shopmart.order.repository.OrderRepository;
import com.shopmart.order.service.OrderService;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderServiceImpl implements OrderService {

    private final OrderRepository orderRepository;
    private final InventoryClient inventoryClient;
    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    @Override
    @Transactional
    public OrderResponse createOrder(OrderRequest request) {
        // TODO Câu 2: Gọi inventory-service qua FeignClient (có Circuit Breaker + fallback)
        //            để lấy thông tin sản phẩm -> tính totalAmount = price * quantity
        ProductDto product = getProductFromInventory(request.getProductId());
        BigDecimal price = (product != null && product.getPrice() != null) ? product.getPrice() : BigDecimal.ZERO;
        BigDecimal totalAmount = price.multiply(BigDecimal.valueOf(request.getQuantity()));

        Order order = Order.builder()
                .customerId(request.getCustomerId())
                .productId(request.getProductId())
                .quantity(request.getQuantity())
                .totalAmount(totalAmount)
                .status(OrderStatus.PENDING)
                .build();
        Order saved = orderRepository.save(order);
        log.info("Created order id={} with status PENDING, totalAmount={}", saved.getId(), totalAmount);

        // TODO Câu 3: Publish OrderEvent (type = ORDER_CREATED) lên Kafka topic "order" để khởi động Saga
        OrderEvent event = OrderEvent.builder()
                .orderId(saved.getId())
                .productId(saved.getProductId())
                .quantity(saved.getQuantity())
                .amount(saved.getTotalAmount())
                .type(SagaEventType.ORDER_CREATED)
                .message("Đơn hàng #" + saved.getId() + " đã được tạo, bắt đầu Saga đặt hàng")
                .build();

        try {
            kafkaTemplate.send(KafkaTopics.ORDER, String.valueOf(saved.getId()), event);
            log.info("[Saga-Start] Đã publish sự kiện ORDER_CREATED cho orderId={}", saved.getId());
        } catch (Exception e) {
            log.error("Lỗi khi gửi sự kiện ORDER_CREATED lên Kafka: {}", e.getMessage(), e);
        }

        return OrderResponse.from(saved);
    }

    @Override
    @CircuitBreaker(name = "inventoryService", fallbackMethod = "fallbackGetProduct")
    public ProductDto getProductFromInventory(Long productId) {
        log.info("Gọi inventory-service qua FeignClient lấy thông tin sản phẩm id={}", productId);
        return inventoryClient.getProductById(productId);
    }

    @Override
    @CircuitBreaker(name = "inventoryService", fallbackMethod = "fallbackGetInstance")
    public String getInventoryInstance() {
        log.info("Gọi inventory-service qua FeignClient lấy instance để kiểm tra Load Balancing");
        return inventoryClient.getInstance();
    }

    /**
     * Fallback method khi Circuit Breaker trạng thái OPEN hoặc gọi inventory-service thất bại.
     * Chống lỗi dây chuyền (Cascading Failure).
     *
     * 3 trạng thái của Resilience4j Circuit Breaker:
     * 1. CLOSED: Bình thường, các request được gửi tới inventory-service. Nếu tỷ lệ lỗi vượt ngưỡng (vd 50%), chuyển sang OPEN.
     * 2. OPEN: Mạch ngắt hoàn toàn. Các request không gọi tới inventory-service mà gọi ngay fallback method. Chờ waitDurationInOpenState (10s) rồi sang HALF-OPEN.
     * 3. HALF-OPEN: Thử nghiệm cho phép một số lượng request giới hạn (vd 2). Nếu thành công -> chuyển về CLOSED; nếu thất bại -> quay lại OPEN.
     */
    public ProductDto fallbackGetProduct(Long productId, Throwable throwable) {
        log.warn("[Circuit Breaker: FALLBACK] inventory-service không khả dụng khi lấy sản phẩm id={}. Lý do: {}",
                productId, throwable.getMessage());
        return ProductDto.builder()
                .id(productId)
                .name("Sản phẩm tạm thời không khả dụng (Circuit Breaker Fallback)")
                .price(BigDecimal.valueOf(10000000))
                .stock(0)
                .build();
    }

    public String fallbackGetInstance(Throwable throwable) {
        log.warn("[Circuit Breaker: FALLBACK] inventory-service không khả dụng. Lý do: {}", throwable.getMessage());
        return "inventory-service đang không khả dụng (Circuit Breaker fallback): " + throwable.getMessage();
    }

    @Override
    public OrderResponse getOrderById(Long id) {
        return OrderResponse.from(findOrder(id));
    }

    @Override
    public List<OrderResponse> getAllOrders() {
        return orderRepository.findAll().stream()
                .map(OrderResponse::from)
                .toList();
    }

    @Override
    @Transactional
    public OrderResponse completeOrder(Long orderId) {
        Order order = findOrder(orderId);
        order.setStatus(OrderStatus.COMPLETED);
        order.setFailureReason(null);
        log.info("Order id={} COMPLETED", orderId);
        return OrderResponse.from(orderRepository.save(order));
    }

    @Override
    @Transactional
    public OrderResponse cancelOrder(Long orderId, String reason) {
        Order order = findOrder(orderId);
        order.setStatus(OrderStatus.CANCELLED);
        order.setFailureReason(reason);
        log.error("Order id={} CANCELLED: {}", orderId, reason);
        return OrderResponse.from(orderRepository.save(order));
    }

    private Order findOrder(Long id) {
        return orderRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Không tìm thấy đơn hàng id=" + id));
    }
}
