package com.example.kafka.consumer;

import java.util.concurrent.CountDownLatch;

import com.protobuf.order.OrderProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;

public class Receiver {

    private static final Logger LOGGER =
        LoggerFactory.getLogger(Receiver.class);

    private CountDownLatch latch = new CountDownLatch(1);

    private OrderProto.Order payload;

    public CountDownLatch getLatch() {
        return latch;
    }

    public OrderProto.Order getPayload() {
        return payload;
    }

    @KafkaListener(topics = "${kafka.topic.helloworld}")
    public void receive(OrderProto.Order payload) {
        LOGGER.info("received payload='{}'", payload);
        this.payload = payload;
        latch.countDown();
    }
}
