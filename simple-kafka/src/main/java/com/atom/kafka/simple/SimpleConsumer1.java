package com.atom.kafka.simple;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author Atom
 */
public class SimpleConsumer1 {

    /**
     * 可以使用 控制台生产者，生产消息
     * kafka-console-producer.sh --broker-list 192.168.56.101:9092 --topic test3
     *
     * @param args
     */
    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put("zookeeper.connect", "192.168.56.101:2181");
        properties.put("group.id", "g1");
        properties.put("zookeeper.session.timeout.ms", "500");
        properties.put("zookeeper.sync.time.ms", "250");
        properties.put("auto.commit.interval.ms", "1000");

        //创建消费者配置对象
        ConsumerConfig conf = new ConsumerConfig(properties);
        //创建消费者连接器
        ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(conf);

        String topic = "test3";
        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topic, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = consumerConnector.createMessageStreams(topicCountMap);

        List<KafkaStream<byte[], byte[]>> kafkaStreams = messageStreams.get(topic);

        for (KafkaStream<byte[], byte[]> kafkaStream : kafkaStreams) {
            ConsumerIterator<byte[], byte[]> iterator = kafkaStream.iterator();
            while (iterator.hasNext()) {
                MessageAndMetadata<byte[], byte[]> next = iterator.next();
                byte[] message = next.message();
                String msg = new String(message);
                System.err.println(msg);
            }
        }

        consumerConnector.shutdown();


    }
}
