package org.training.spark.kafka;

import java.util.Properties;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * 创建生产者（过时的API）
 */
public class MyProducerold {

    @SuppressWarnings("deprecation")
    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put("metadata.broker.list", "hadoop100:9092");
        properties.put("request.required.acks", "1");
        properties.put("serializer.class", "kafka.serializer.StringEncoder");

        Producer<Integer, String> producer = new Producer<Integer,String>(new ProducerConfig(properties));

        KeyedMessage<Integer, String> message = new KeyedMessage<Integer, String>("first", "hello world");
        producer.send(message );
    }
}
