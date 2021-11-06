package com.atom.kafka.simple;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

/**
 * @author Atom
 */
public class SimpleKafkaConsumer {

    /**
     * 可以使用 控制台生产者，生产消息
     * kafka-console-producer.sh --broker-list 192.168.56.101:9092 --topic test1
     *
     * @param args
     */
    public static void main(String[] args) {

        // Kafka消费者三个属性必须配置（broker清单、key的反序列化器、value的反序列化器）
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092,192.168.56.102:9092,192.168.56.103:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        // 消费者组并非完全必须配置
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "myGroup1");

        KafkaConsumer<Object, Object> consumer = new KafkaConsumer<>(properties);

        // 消费者订阅topic（可以订阅多个）
        consumer.subscribe(Collections.singletonList("test1"));
        while (true) {
            //拉取消息，超时时间：500毫秒
            ConsumerRecords<Object, Object> records = consumer.poll(500);
            for (ConsumerRecord<Object, Object> record : records) {
                String topic = record.topic();
                int partition = record.partition();
                long offset = record.offset();
                Object key = record.key();
                Object value = record.value();
                System.err.printf("topic：%s, 分区：%d，偏移量：%d，key：%s，value：%s%n", topic, partition, offset, key, value);
                //do work, use threadPool
            }
        }


    }
}
