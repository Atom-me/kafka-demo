package com.atom.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Objects;


/**
 * @author Atom
 */
@Component
public class KafkaFileProducer implements ApplicationRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaFileProducer.class);

    @Autowired
    private KafkaTemplate<byte[], byte[]> kafkaTemplate;

    @Value("${kafka.topic}")
    private String kafkaTopic;

    @Value("${kafka.chunk-size}")
    private int chunkSize;


    @Override
    public void run(ApplicationArguments args) {
        String sourceFolderPath = "/Users/atom/temp/";

        File folder = new File(sourceFolderPath);
        File[] files = folder.listFiles();
        for (File file : files) {
            if (file.isFile()) {
                String absolutePath = file.getAbsolutePath();
                String sourceFileName = file.getName();
                sendToKafka(absolutePath, sourceFileName);
            }
        }
    }

    private void sendToKafka(String sourceFileFullName, String sourceFileName) {
        try (FileInputStream inputStream = new FileInputStream(sourceFileFullName)) {
            byte[] buffer = new byte[chunkSize];
            int bytesRead;
            int chunkNumber = 0;

            long fileSize = inputStream.available();
            // 向上取整
            int totalChunks = (int) Math.ceil((double) fileSize / chunkSize);
            LOGGER.info("fileName ========: [{}], fileSize [{}] totalChunks [{}]", sourceFileFullName, fileSize, totalChunks);

            while ((bytesRead = inputStream.read(buffer)) != -1) {
                byte[] chunkData = new byte[bytesRead];
                System.arraycopy(buffer, 0, chunkData, 0, bytesRead);

                ChunkMetadata metadata = new ChunkMetadata(sourceFileName, chunkNumber, totalChunks, bytesRead);
                byte[] serializedMetadata = new ObjectMapper().writeValueAsBytes(metadata);

                // 需要确保同一个文件的分块被顺序处理，使用相同的分区键
                // 使用文件名的哈希值与 Kafka 主题的分区数取模（取余数）来获取一个有效的分区键。这样可以确保分区键的值在分区范围内。
                // todo kafka集群 扩容 与 partitions 增加
                int partitionKey = Math.abs(Objects.hash(sourceFileFullName)) % kafkaTemplate.partitionsFor(kafkaTopic).size();
                LOGGER.info("partitionKey ========: [{}]", partitionKey);

                ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(kafkaTopic, partitionKey, serializedMetadata, chunkData);
                kafkaTemplate.send(producerRecord);
                LOGGER.info("fileName ========: [{}], fileSize [{}] totalChunks [{}] chunkNumber [{}] chunkSize [{}]", sourceFileFullName, fileSize, totalChunks, chunkNumber, chunkData.length);
                chunkNumber++;
            }
        } catch (IOException e) {
            LOGGER.error("send file error  occurs", e);
        }
    }


    private static class ChunkMetadata {
        private String fileName;
        private int chunkOrder;
        private int totalChunks;
        private int chunkSize;

        public ChunkMetadata(String fileName, int chunkOrder, int totalChunks, int chunkSize) {
            this.fileName = fileName;
            this.chunkOrder = chunkOrder;
            this.totalChunks = totalChunks;
            this.chunkSize = chunkSize;
        }

        public String getFileName() {
            return fileName;
        }

        public void setFileName(String fileName) {
            this.fileName = fileName;
        }

        public int getChunkOrder() {
            return chunkOrder;
        }

        public void setChunkOrder(int chunkOrder) {
            this.chunkOrder = chunkOrder;
        }

        public int getTotalChunks() {
            return totalChunks;
        }

        public void setTotalChunks(int totalChunks) {
            this.totalChunks = totalChunks;
        }

        public int getChunkSize() {
            return chunkSize;
        }

        public void setChunkSize(int chunkSize) {
            this.chunkSize = chunkSize;
        }
    }
}