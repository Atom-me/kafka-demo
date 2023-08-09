package com.atom.kafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * @author Atom
 */
@Configuration
public class KafkaTopicConfig {

    /**
     * 程序启动自动创建topic
     *
     * @return
     */
    @Bean
    public NewTopic topic() {
        return TopicBuilder
                .name("wikimedia_recentchange")
                .build();
    }


    /**
     * 程序启动自动创建topic
     *
     * @return
     */
    @Bean
    public NewTopic p_topic() {
        return TopicBuilder
                .name("p_topic")
                .partitions(5)
                .build();
    }
}
