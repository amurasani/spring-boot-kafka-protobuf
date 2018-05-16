package com.example.kafka.controller;

import com.example.kafka.producer.Sender;
import com.protobuf.order.OrderProto;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@RequestMapping(path="/api/kafka")
public class MainController {

    @Autowired
    private Sender producer;

    @Value("${kafka.topic.order}")
    private String topic;

    @GetMapping(path="/produce")
    public @ResponseBody String sendProtobuffEventToKafka() {
        OrderProto.Order.Builder orderBuilder = OrderProto.Order.newBuilder();
        orderBuilder.setName("Order to produce protobuff Event.");
        orderBuilder.setOrderAddress("3150 Sabre Dr, Southlake 76092");
        orderBuilder.setOrderId(3456);
        producer.send(topic, orderBuilder.build());
        return "Order event sent to kafka";
    }
}
