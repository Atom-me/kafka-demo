package com.atom.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * @author Atom
 */
@Component
public class KafkaConsumer {

    @KafkaListener(topics = "myTopic", groupId = "${spring.kafka.consumer.group-id}")
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
}
