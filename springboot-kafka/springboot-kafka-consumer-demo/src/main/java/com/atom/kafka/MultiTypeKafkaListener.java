package com.atom.kafka;

import com.atom.kafka.model.Farewell;
import com.atom.kafka.model.Greeting;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * @author Atom
 */
@Component
@KafkaListener(id = "multiGroup", topics = "mt_topic")
public class MultiTypeKafkaListener {


    @KafkaHandler
    public void handleGreeting(Greeting greeting, Acknowledgment acknowledgment) {
        System.out.println("Greeting received:************************** " + greeting);
        acknowledgment.acknowledge();
    }

    @KafkaHandler
    public void handleFarewell(Farewell farewell, Acknowledgment acknowledgment) {
        System.out.println("Farewell received:************************** " + farewell);
        acknowledgment.acknowledge();
    }

    /**
     * Default Message Handler
     *
     * @param object
     */
    @KafkaHandler(isDefault = true)
    public void unknown(Object object, Acknowledgment acknowledgment) {
        System.out.println("Unknown type received:************************** " + object);
        acknowledgment.acknowledge();
    }
}