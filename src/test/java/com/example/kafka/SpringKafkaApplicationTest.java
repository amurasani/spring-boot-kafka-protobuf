package com.example.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.TimeUnit;

import com.protobuf.order.OrderProto;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import com.example.kafka.consumer.Receiver;
import com.example.kafka.producer.Sender;

@RunWith(SpringRunner.class)
@SpringBootTest
@DirtiesContext
public class SpringKafkaApplicationTest {

    private static final String HELLOWORLD_TOPIC = "helloworld.t";

    @ClassRule
    public static KafkaEmbedded embeddedKafka =
        new KafkaEmbedded(1, true, HELLOWORLD_TOPIC);

    @Autowired
    private Receiver receiver;

    @Autowired
    private Sender sender;

    @Test
    public void testReceive() throws Exception {
        OrderProto.Order.Builder builder = OrderProto.Order.newBuilder();
        builder.setName("Anil Murasani");
        builder.setOrderAddress("3150 Sabre Dr SouthLake Tx");
        builder.setOrderId(1234);
        OrderProto.Order protoOrder = builder.build();

        sender.send(HELLOWORLD_TOPIC, protoOrder);

        receiver.getLatch().await(10000, TimeUnit.MILLISECONDS);
        assertThat(receiver.getLatch().getCount()).isEqualTo(0);
        assertThat(receiver.getPayload()).isEqualTo(protoOrder);
    }
}

