package com.atom.kafka;

import com.atom.kafka.model.Greeting;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.kafka.support.KafkaHeaders;


/**
 * @author Atom
 */
@Component
public class KafkaConsumer {

    @KafkaListener(topics = "m_topic", groupId = "${spring.kafka.consumer.group-id}")
    public void obtainMessage(ConsumerRecord<String, String> consumerRecord) {
        System.out.println("obtainMessage invoked");

        String topic = consumerRecord.topic();
        String key = consumerRecord.key();
        String value = consumerRecord.value();
        int partition = consumerRecord.partition();
        long timestamp = consumerRecord.timestamp();
        System.out.println("=================================");
        System.out.println("topic: " + topic);
        System.out.println("key: " + key);
        System.out.println("value: " + value);
        System.out.println("partition: " + partition);
        System.out.println("timestamp: " + timestamp);
        System.out.println("=================================");


    }

    @KafkaListener(topics = "g_topic")
    public void greetingMessage(Greeting greeting) {
        System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
        System.out.println(greeting);
        System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++");

    }


    @KafkaListener(topics = "f_topic", containerFactory = "filterKafkaListenerContainerFactory")
    public void listenWithFilter(String message) {
        System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        System.out.println("Received Message in filtered listener: " + message);
        System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
    }


    /**
     * 注解方式获取消息头及消息体
     *
     * @param message
     * @param partition
     */
    @KafkaListener(topicPartitions = @TopicPartition(topic = "${partitioned.topic.name}", partitions = {"0", "3"}), containerFactory = "partitionsKafkaListenerContainerFactory")
    public void listenToPartition(@Payload String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic, @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
        System.out.println("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
        System.out.println("Topic: " + topic + " Received Message:" + message + " from partition:" + partition);
        System.out.println("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
    }
}
