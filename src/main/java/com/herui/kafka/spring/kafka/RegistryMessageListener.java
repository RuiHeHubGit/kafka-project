package com.herui.kafka.spring.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.MessageListener;

public class RegistryMessageListener implements MessageListener<Integer, String> {
    public void onMessage(ConsumerRecord<Integer, String> integerStringConsumerRecord) {
        System.out.println("receive message: "+integerStringConsumerRecord.value());
    }
}
