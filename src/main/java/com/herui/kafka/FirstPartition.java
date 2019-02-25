package com.herui.kafka;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Created by HeRui on 2019/2/26.
 */
public class FirstPartition implements Partitioner {
    private Random random = new Random();

    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitionInfos = cluster.partitionsForTopic(topic);
        int partitionNum = 0;
        if(key == null){
            partitionNum = random.nextInt(partitionInfos.size());
        }else {
            partitionNum = Math.abs((key.hashCode())%partitionInfos.size());
        }
        System.out.println("key ->"+key+"->value->"+value+"->"+partitionNum);
        return partitionNum;
    }

    public void close() {

    }

    public void configure(Map<String, ?> map) {

    }
}
