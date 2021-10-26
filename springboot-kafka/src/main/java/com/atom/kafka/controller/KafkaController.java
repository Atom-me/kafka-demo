package com.atom.kafka.controller;

import com.atom.kafka.KafkaMessage;
import com.atom.kafka.KafkaProducer;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

import java.util.Date;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

/**
 * @author Atom
 */
@RestController
@RequestMapping(value = "/kafka", produces = APPLICATION_JSON_VALUE)
public class KafkaController {

    @Resource
    private KafkaProducer kafkaProducer;

    @RequestMapping(value = "/message", method = RequestMethod.GET)
    public KafkaMessage sendKafkaMessage(@RequestParam(name = "id") Long id,
                                         @RequestParam(name = "username") String username,
                                         @RequestParam(name = "password") String password) {
        System.out.println("sendKafkaMessage invoked!");
        KafkaMessage kafkaMessage = new KafkaMessage();
        kafkaMessage.setId(id);
        kafkaMessage.setUsername(username);
        kafkaMessage.setPassword(password);
        kafkaMessage.setDate(new Date());

        this.kafkaProducer.sendKafkaMessage(kafkaMessage);

        return kafkaMessage;
    }

}
