package com.atom.kafka;

import com.atom.kafka.model.Farewell;
import com.atom.kafka.model.Greeting;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * @author Atom
 */
@Component
@KafkaListener(id = "multiGroup", topics = "mt_topic")
public class MultiTypeKafkaListener {

    @KafkaHandler
    public void handleGreeting(Greeting greeting) {
        System.out.println("Greeting received:************************** " + greeting);
    }

    @KafkaHandler
    public void handleF(Farewell farewell) {
        System.out.println("Farewell received:************************** " + farewell);
    }

    @KafkaHandler(isDefault = true)
    public void unknown(Object object) {
        System.out.println("Unkown type received:************************** " + object);
    }
}