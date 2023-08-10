package com.atom.kafka.controller;

import com.atom.kafka.KafkaProducer;
import com.atom.kafka.UserMember;
import com.atom.kafka.model.Greeting;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
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

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaController.class);

    @Resource
    private KafkaProducer kafkaProducer;

    /**
     * get request publish kafka message.
     * <p>
     * 直接发送java对象
     *
     * @param id
     * @param username
     * @param password
     * @return
     */
    @GetMapping(value = "/message")
    public UserMember sendKafkaMessage(@RequestParam(name = "id") Long id,
                                       @RequestParam(name = "username") String username,
                                       @RequestParam(name = "password") String password) throws JsonProcessingException {
        LOGGER.info("sendKafkaMessage invoked!");

        // 发送字符串类消息
        UserMember userMember = new UserMember();
        userMember.setId(id);
        userMember.setUsername(username);
        userMember.setPassword(password);
        userMember.setDate(new Date());
        ObjectMapper objectMapper = new ObjectMapper();
        String memberJsonStr = objectMapper.writeValueAsString(userMember);
        kafkaProducer.sendMessage(memberJsonStr);

        // 发送java对象类消息
        kafkaProducer.sendGreetingMessage(new Greeting("Greetings", "Atom!"));

        // 发送过滤类消息（生产端就是发送的普通消息，消费者端处理消息过滤逻辑）
        kafkaProducer.sendMessageToFiltered("Hello Atom!");
        kafkaProducer.sendMessageToFiltered("Hello World!");
        kafkaProducer.sendMessageToFiltered("Hello Word!");

        // 发送多种类型消息
        kafkaProducer.sendMulTypeMessages();


        // 向指定partition发送消息
        for (int i = 0; i < 5; i++) {
            kafkaProducer.sendMessageToPartition("Hello To Partitioned Topic!", i);
        }
        return userMember;
    }


}
