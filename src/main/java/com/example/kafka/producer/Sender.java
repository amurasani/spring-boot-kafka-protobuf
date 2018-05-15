package com.example.kafka.producer;

import com.protobuf.order.OrderProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;

public class Sender {

    private static final Logger LOGGER =
        LoggerFactory.getLogger(Sender.class);

    @Autowired
    private KafkaTemplate<String, OrderProto.Order> kafkaTemplate;

    public void send(String topic, OrderProto.Order payload) {
        LOGGER.info("sending payload='{}' to topic='{}'", payload, topic);
        kafkaTemplate.send(topic, payload);
    }
}
