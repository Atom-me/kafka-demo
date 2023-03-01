package com.atom.kafka.simple;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

/**
 * @author Atom
 */
public class SimpleKafkaConsumer {

    /**
     * 可以使用 控制台生产者，生产消息
     * kafka-console-producer.sh --broker-list 192.168.56.101:9092 --topic test1
     *
     * @param args
     */
    public static void main(String[] args) {

        // Kafka消费者三个属性必须配置（broker清单、key的反序列化器、value的反序列化器）
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092,192.168.56.102:9092,192.168.56.103:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //这里我们不允许自动提交，所以把enable.auto.commit设置为false
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        // 消费者组并非完全必须配置
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "myGroup1");

        KafkaConsumer<Object, Object> consumer = new KafkaConsumer<>(properties);

        // 消费者订阅topic（可以订阅多个）
        consumer.subscribe(Collections.singletonList("test1"));
        while (true) {
            //拉取消息，超时时间：500毫秒
            ConsumerRecords<Object, Object> records = consumer.poll(500);
            for (ConsumerRecord<Object, Object> record : records) {
                String topic = record.topic();
                int partition = record.partition();
                long offset = record.offset();
                Object key = record.key();
                Object value = record.value();
                System.err.printf("topic：%s, 分区：%d，偏移量：%d，key：%s，value：%s%n", topic, partition, offset, key, value);
                //do work, use threadPool
            }
            /**
             * commitSync（同步提交）和commitAsync（异步提交）是手动提交offset的两种方式。
             * 两者的相同点是：都会将本次poll的一批数据里面最高的偏移量提交；
             *
             * 不同点是：commitSync会失败重试，一直到提交成功（如果由于不可恢复原因导致，也会提交失败）；而commitAsync则没有失败重试机制，故有可能提交失败。
             *
             * 注意：虽然commitAsync没有失败重试机制，但是咱们实际工作中还是用它比较多，因为commitAsync虽然可能提交失败，但是失败后下一次重新提交（变相的相当于重试），对数据并没有什么影响，而且异步提交的延迟较同步提交较低。
             */
            consumer.commitAsync();
//            consumer.commitSync();
        }

        /**
         * 代码的最后为什么不和生产者一样进行close呢？
         *
         * 其实就像spark streaming和flink streaming一样，对于需要一直消费的角色，是没有close方法的，要么close是内嵌在别的方法之内，要么就是不需要close。
         * 比如这里的kafka的consumer，咱们是用来测试基础demo框架的，因此不需要close，让它一直读对应topic的数据就好，
         * 实际生产中，kafka和spark streaming或者flink对接，是会一直进行数据的消费的，也不需要close。
         */


    }
}
