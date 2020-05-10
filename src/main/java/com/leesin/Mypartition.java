package com.leesin;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * @description:
 * @author: Leesin Dong
 * @date: Created in 2020/5/10 0010 16:37
 * @modified By:
 */
//自定义分区
public class Mypartition implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        System.out.println("enter");
        List<PartitionInfo> list=cluster.partitionsForTopic(topic);
        int leng=list.size();
        //空的时候随机
        if(key==null){
            Random random=new Random();
            return random.nextInt(leng);
        }
        //不空的时候取模
        return Math.abs(key.hashCode())%leng;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
