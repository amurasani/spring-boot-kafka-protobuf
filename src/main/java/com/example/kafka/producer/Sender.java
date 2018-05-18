package com.example.kafka.producer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;

public class Sender {

    @Autowired
    private KafkaTemplate<String, byte[]> kafkaTemplate;

    public void send(String topic, byte[] payload) {
        kafkaTemplate.send(topic, payload);
    }
}
