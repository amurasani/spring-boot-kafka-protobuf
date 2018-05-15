package com.example.kafka.serializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.kafka.common.serialization.Deserializer;

import com.protobuf.order.OrderProto;

public class OrderDeserializer extends Adapter implements Deserializer<OrderProto.Order> {
    private static final Logger LOG = LoggerFactory.getLogger(OrderDeserializer.class);

    @Override
    public OrderProto.Order deserialize(final String topic, byte[] data) {
        try {
            return OrderProto.Order.parseFrom(data);
        } catch (final InvalidProtocolBufferException e) {
            LOG.error("Message: "+ new String(data));
            LOG.error("Received unparseable message", e);
            throw new RuntimeException("Received unparseable message " + e.getMessage(), e);
        }
    }

}
