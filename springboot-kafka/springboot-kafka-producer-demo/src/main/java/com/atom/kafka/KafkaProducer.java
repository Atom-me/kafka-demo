package com.atom.kafka;

import com.atom.kafka.model.Farewell;
import com.atom.kafka.model.Greeting;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * @author Atom
 */
@Component
public class KafkaProducer {

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
        kafkaTemplate.send(topicName, message);
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
