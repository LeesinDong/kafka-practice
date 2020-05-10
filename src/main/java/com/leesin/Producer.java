package com.leesin;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @description:
 * @author: Leesin Dong
 * @date: Created in 2020/5/10 0010 15:08
 * @modified By:
 */
public class Producer extends Thread {
    //producer api
    KafkaProducer<Integer, String> producer;
    //主题
    String topic;

    public Producer(String topic) {
        Properties properties = new Properties();
        //ip port
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "120.78.190.230:9092");
        //client id
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "producer");
        //partition
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.leesin.Mypartition");
        //序列化
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //异步发送需要两个属性
        // properties.put(ProducerConfig.BATCH_SIZE_CONFIG,"");
        // properties.put(ProducerConfig.LINGER_MS_CONFIG,"");

        //连接的字符串
        //通过工厂
        //new
        producer = new KafkaProducer<>(properties);
        this.topic = topic;
    }

    @Override
    public void run() {
        int num = 0;
        while (num < 20) {
            String msg = "kafka msg:" + num;
            //第二个参数key是用来 决定分区的，根据这个key得到一个随机值放到对应的分区中
            producer.send(new ProducerRecord<>(topic,1,msg),(metadata,exception)->{
                System.out.println(metadata.offset() + "->" + metadata.partition() + "->" + metadata.topic());
            });
            try {
                TimeUnit.SECONDS.sleep(2);
                ++num;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    //异步
    // public void run() {
    //     int num=0;
    //     while(num<50){
    //         String msg="pratice test message:"+num;
    //         try {
    //             producer.send(new ProducerRecord<>(topic, msg), new Callback() {
    //                 @Override
    //                 public void onCompletion(RecordMetadata recordMetadata,
    //                                          Exception e) {
    //                     System.out.println("callback: "+recordMetadata.offset()+"->"+recordMetadata.partition());
    //                 }
    //             });
    //             TimeUnit.SECONDS.sleep(2);
    //             num++;
    //         } catch (InterruptedException e) {
    //             e.printStackTrace();
    //         }
    //     }
    // }

    public static void main(String[] args) {
        new Producer("test-partition").start();
    }
}
