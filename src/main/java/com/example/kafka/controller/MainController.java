package com.example.kafka.controller;

import com.example.kafka.producer.Sender;
import com.protobuf.order.OrderProto;
import com.protobuf.schedule.ScheduleProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@RequestMapping(path="/api/kafka")
public class MainController {

    private static final Logger LOGGER =
        LoggerFactory.getLogger(Sender.class);

    @Autowired
    private Sender producer;

    @Value("${kafka.topic.order}")
    private String orderTopic;

    @Value("${kafka.topic.schedule}")
    private String scheduleTopic;

    @GetMapping(path="/produce/order")
    public @ResponseBody String sendOrderProtobuffEventToKafka() {
        OrderProto.Order.Builder orderBuilder = OrderProto.Order.newBuilder();
        orderBuilder.setName("Order to produce protobuff Event.");
        orderBuilder.setOrderAddress("3150 Sabre Dr, Southlake 76092");
        orderBuilder.setOrderId(3456);
        OrderProto.Order order = orderBuilder.build();
        LOGGER.info("sending payload='{}' to topic='{}'", order, orderTopic);
        producer.send(orderTopic, order.toByteArray());
        return "Order event sent to kafka. Topic: " + orderTopic;
    }

    @GetMapping(path="/produce/schedule")
    public @ResponseBody String sendScheduleProtobuffEventToKafka() {
        ScheduleProto.Schedule.Builder scheduleBuilder = ScheduleProto.Schedule.newBuilder();
        scheduleBuilder.setFlightNumber("AA169");
        scheduleBuilder.setHosted(true);
        scheduleBuilder.setVersion(12223232);
        scheduleBuilder.setId(1234);
        ScheduleProto.LegInformation.Builder legInfoBuilder = ScheduleProto.LegInformation.newBuilder();
        legInfoBuilder.setArrivalCityCode("LAX");
        legInfoBuilder.setArrivalTerminalCode("T3");
        scheduleBuilder.addLegs(legInfoBuilder);
        ScheduleProto.Schedule schedule = scheduleBuilder.build();
        LOGGER.info("sending payload='{}' to topic='{}'", schedule, scheduleTopic);
        producer.send(scheduleTopic, schedule.toByteArray());
        return "Schedule event sent to kafka. Topic: " + scheduleTopic;
    }

}
