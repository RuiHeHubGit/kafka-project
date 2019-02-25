package com.herui.kafka;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Created by HeRui on 2019/2/26.
 */
public class FirstKafkaProducer {
    private final KafkaProducer<Integer,String> producer;
    private final String topic;
    private final boolean isAsync;

    public FirstKafkaProducer(String topic, boolean isAsync) {
        this.topic = topic;
        this.isAsync = isAsync;
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.1.10:9092");
        properties.put(ProducerConfig.CLIENT_ID_CONFIG,"KafkaProducerDemo");
        properties.put(ProducerConfig.ACKS_CONFIG,"-1");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.IntegerSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"com.herui.kafka.FirstPartition");
        producer = new KafkaProducer<Integer, String>(properties);
    }

    public void run() {
        int num = 0;
        while (num <50){
            String message = "message_"+num;
            System.out.println("begin run message:"+message);
            if (isAsync){
                producer.send(new ProducerRecord<Integer, String>(topic, message), new Callback() {
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (metadata!=null){
                            System.out.println("async-offset:"+metadata.offset()+" ->partition"+metadata.partition());
                        }
                    }
                });
            }else {
                try {
                    //future
                    RecordMetadata  recordMetadata = producer.send(new ProducerRecord<Integer, String>(topic,message)).get();
                    System.out.println("async-offset:"+recordMetadata.offset()+" ->partition"+recordMetadata.partition());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }
            num++;
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        FirstKafkaProducer producer = new FirstKafkaProducer("first", false);
        producer.run();
    }
}
