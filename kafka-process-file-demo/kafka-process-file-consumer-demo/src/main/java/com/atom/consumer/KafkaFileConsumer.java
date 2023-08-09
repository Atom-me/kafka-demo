package com.atom.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Atom
 */
@Component
public class KafkaFileConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaFileConsumer.class);

    private final Map<Integer, byte[]> chunksMap = new ConcurrentHashMap<>();
    private int totalChunks = -1;

    private String fileName;


    @Value("${kafka.consumer.output-path}")
    private String outputPath;

    @KafkaListener(topics = "${kafka.topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void listen(ConsumerRecord<byte[], byte[]> record, Acknowledgment acknowledgment) {

        try {
            byte[] key = record.key();
            byte[] value = record.value();

            // 反序列化获取key(业务属性)
            ChunkMetadata chunkMetadata = new ObjectMapper().readValue(key, ChunkMetadata.class);
            LOGGER.info("received chunkMetadata:=================================================================================== [{}]", chunkMetadata);

            if (totalChunks == -1) {
                totalChunks = chunkMetadata.getTotalChunks();
                fileName = chunkMetadata.getFileName();
            }

            // 将文件分块内容存入内存
            chunksMap.put(chunkMetadata.getChunkOrder(), value);

            if (chunksMap.size() == totalChunks) {
                // 获取到当前文件所有的分块，重新组装文件
                reassembleFile(fileName);
                // 提交位移
                acknowledgment.acknowledge();
            }
        } catch (IOException e) {
            LOGGER.error("consume file error occurs", e);
            // todo 文件的其中一个分块处理失败的情况
        }
    }

    private void reassembleFile(String fileName) {
        String newFilePath = outputPath + fileName;
        try (FileOutputStream outputStream = new FileOutputStream(newFilePath)) {
            // 按顺序拼接
            for (int i = 0; i < totalChunks; i++) {
                byte[] chunkData = chunksMap.get(i);
                outputStream.write(chunkData);
            }
            outputStream.flush();
            LOGGER.info("Original file  [{}] reassembled successfully.", fileName);
        } catch (IOException e) {
            LOGGER.error(" reassemble file error occurs", e);
        } finally {
            chunksMap.clear();
            totalChunks = -1;
            fileName = null;
        }
    }

    private static class ChunkMetadata {
        private String fileName;
        private int chunkOrder;
        private int totalChunks;
        private int chunkSize;

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

        @Override
        public String toString() {
            return "ChunkMetadata{" +
                    "fileName='" + fileName + '\'' +
                    ", chunkOrder=" + chunkOrder +
                    ", totalChunks=" + totalChunks +
                    ", chunkSize=" + chunkSize +
                    '}';
        }
    }
}