package com.atom.kafka.simple;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

/**
 * @author Atom
 */
public class SimpleProducer0_oldApi {
    /**
     * 使用 console 消费者消费消息
     * kafka-console-consumer.sh --zookeeper 192.168.56.101 --topic test3 --from-beginning
     *
     * @param args
     */
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("metadata.broker.list", "192.168.56.101:9092,192.168.56.102:9092,192.168.56.103:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");
        Producer<Integer, String> producer = new Producer<>(new ProducerConfig(props));

        String topic = "test3";
        String messageStr = "hello kafka from mac";
        KeyedMessage<Integer, String> data = new KeyedMessage<>(topic, messageStr);
        producer.send(data);
        producer.close();
        System.out.println("over");
    }

    /**
     *
     */
}
