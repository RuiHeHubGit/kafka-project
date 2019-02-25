package com.herui.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created by HeRui on 2019/2/26.
 */
public class FirstKafkaConsumer {
    private KafkaConsumer<Integer, String> kafkaConsumer;

    public FirstKafkaConsumer(String topic) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.1.10:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"FirstKafkaConsumer");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"true");
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"1000");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.IntegerDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        kafkaConsumer = new KafkaConsumer<Integer, String>(properties);

        TopicPartition topicPartition = new TopicPartition(topic, 0);
        kafkaConsumer.assign(Arrays.asList(topicPartition));
    }

    public void run() {
        while (true){
            ConsumerRecords<Integer,String> consumerRecords =  kafkaConsumer.poll(1000);
            for (ConsumerRecord<Integer,String> record:consumerRecords){
                System.out.println(record.partition()+"->message receive :"+record.key()+":"+record.value());
            }
        }
    }

    public static void main(String[] args) {
        FirstKafkaConsumer consumer = new FirstKafkaConsumer("first");
        consumer.run();
    }
}
