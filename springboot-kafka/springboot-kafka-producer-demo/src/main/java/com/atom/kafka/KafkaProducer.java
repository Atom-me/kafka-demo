package com.atom.kafka;

import com.alibaba.fastjson.JSON;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * @author Atom
 */
@Component
public class KafkaProducer {

    @Resource
    private KafkaTemplate<String, String> kafkaTemplate;

    public void sendKafkaMessage(KafkaMessage kafkaMessage) {
        this.kafkaTemplate.send("myTopic", JSON.toJSONString(kafkaMessage));
    }
}
