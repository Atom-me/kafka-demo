package com.atom.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author Atom
 */
public class SimpleProducer1 {
    /**
     * 使用 console 消费者消费消息
     * kafka-console-consumer.sh --zookeeper 192.168.56.101 --topic test3 --from-beginning
     *
     * @param args
     */
    public static void main(String[] args) {
        Properties props = new Properties();
        // broker list
        props.put("bootstrap.servers", "192.168.56.101:9092,192.168.56.102:9092,192.168.56.103:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");


        Producer<String, String> producer = new KafkaProducer<>(props);
        String topic = "test3";
        String messageStr = "hello kafka from mac";
        for (int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord<>(topic, Integer.toString(i), messageStr + "-" + i));
        }
        producer.close();
        System.out.println("over");
    }

    /**
     *
     */
}
