package com.example.kafka.serializer;

import com.protobuf.order.OrderProto;
import org.apache.kafka.common.serialization.Serializer;

public class OrderSerializer extends Adapter implements Serializer<OrderProto.Order> {
    @Override
    public byte[] serialize(final String topic, final OrderProto.Order data) {
        return data.toByteArray();
    }
}
