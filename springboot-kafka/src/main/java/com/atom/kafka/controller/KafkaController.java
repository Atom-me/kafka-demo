package com.atom.kafka.controller;

import com.atom.kafka.KafkaMessage;
import com.atom.kafka.KafkaProducer;
import org.springframework.web.bind.annotation.*;

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

    /**
     * get request publish kafka message.
     *
     * @param id
     * @param username
     * @param password
     * @return
     */
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


    /**
     * curl -X POST -H "Content-Type: application/json" -d '{"id":5,"username":"lisi","password":"2312432"}' 'http://localhost:8080/kafka/message2'
     * <p>
     * post request publish kafka message.
     *
     * @param kafkaMessage
     * @return
     */
    @RequestMapping(value = "/message2", method = RequestMethod.POST)
    public KafkaMessage sendKafkaMessage2(@RequestBody KafkaMessage kafkaMessage) {
        System.out.println("sendKafkaMessage2 invoked!");
        kafkaMessage.setDate(new Date());

        this.kafkaProducer.sendKafkaMessage(kafkaMessage);
        return kafkaMessage;
    }

}
