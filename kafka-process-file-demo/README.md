# 一：

```java
Send failed; nested exception is org.apache.kafka.common.errors.RecordTooLargeException: 
The message is 2097333 bytes when serialized which is larger than 1048576, 
which is the value of the max.request.size configuration.
  
```

server.properties

```properties
# 3*1024*1024=3145728 3MB
message.max.bytes=3145728

```

java kafka producer 

```java
//  max.request.size 3MB
props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 3 * 1024 * 1024);
```

