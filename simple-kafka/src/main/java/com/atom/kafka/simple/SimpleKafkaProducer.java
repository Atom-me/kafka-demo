package com.atom.kafka.simple;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author Atom
 */
public class SimpleKafkaProducer {
    /**
     * 可以使用 console 消费者消费消息
     * kafka-console-consumer.sh --zookeeper 192.168.56.101 --topic test3 --from-beginning
     *
     * @param args
     */
    public static void main(String[] args) {
        Properties props = new Properties();
        // 生产者三个属性必须指定（broker清单、key序列化器、value序列化器）
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092,192.168.56.102:9092,192.168.56.103:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        String topic = "test1";
        String messageStr = "hello kafka from mac";
        // 发送消息
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, Integer.toString(i), messageStr + "-" + i);
            producer.send(record);
            System.err.println(i + ", message is sent");
        }
        producer.close();
        System.err.println("over");
    }

}
