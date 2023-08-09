package com.atom.kafka.controller;

import com.atom.kafka.KafkaMessage;
import com.atom.kafka.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaController.class);

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
    @GetMapping(value = "/message")
    public KafkaMessage sendKafkaMessage(@RequestParam(name = "id") Long id,
                                         @RequestParam(name = "username") String username,
                                         @RequestParam(name = "password") String password) {
        LOGGER.info("sendKafkaMessage invoked!");

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
    @PostMapping(value = "/message2")
    public KafkaMessage sendKafkaMessage2(@RequestBody KafkaMessage kafkaMessage) {
        LOGGER.info("sendKafkaMessage2 invoked!");

        kafkaMessage.setDate(new Date());

        this.kafkaProducer.sendKafkaMessage(kafkaMessage);
        return kafkaMessage;
    }

}
