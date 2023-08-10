package com.atom.kafka;

import com.atom.kafka.model.Farewell;
import com.atom.kafka.model.Greeting;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.FailureCallback;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.util.concurrent.SuccessCallback;

/**
 * @author Atom
 */
@Component
public class KafkaProducer {


    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducer.class);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    @Autowired
    private KafkaTemplate<String, Greeting> greetingKafkaTemplate;
    @Autowired
    private KafkaTemplate<String, Object> multiTypeKafkaTemplate;


    @Value(value = "${message.topic.name}")
    private String topicName;

    @Value(value = "${partitioned.topic.name}")
    private String partitionedTopicName;

    @Value(value = "${filtered.topic.name}")
    private String filteredTopicName;

    @Value(value = "${greeting.topic.name}")
    private String greetingTopicName;

    @Value(value = "${multi.type.topic.name}")
    private String multiTypeTopicName;

    public void sendMessage(String message) {
        ListenableFuture<SendResult<String, String>> send = kafkaTemplate.send(topicName, message);
        send.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
            @Override
            public void onFailure(Throwable ex) {
                LOGGER.error("send message failure ", ex);
            }

            @Override
            public void onSuccess(SendResult<String, String> result) {
                ProducerRecord<String, String> producerRecord = result.getProducerRecord();
                RecordMetadata recordMetadata = result.getRecordMetadata();
                LOGGER.info("send message success, producerRecord: [{}] , recordMetadata: [{}]", producerRecord, recordMetadata);
            }
        });

    }

    public void sendGreetingMessage(Greeting greeting) {
        greetingKafkaTemplate.send(greetingTopicName, greeting);
    }

    public void sendMessageToPartition(String message, int partition) {
        kafkaTemplate.send(partitionedTopicName, partition, null, message);
    }

    public void sendMessageToFiltered(String message) {
        kafkaTemplate.send(filteredTopicName, message);
    }

    public void sendMulTypeMessages() {
        multiTypeKafkaTemplate.send(multiTypeTopicName, new Greeting("Greetings", "World!"));
        multiTypeKafkaTemplate.send(multiTypeTopicName, new Farewell("Farewell", 25));
        multiTypeKafkaTemplate.send(multiTypeTopicName, "Simple string message");
    }


}
