package org.training.spark.kafkaProducer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * 自定义分区（新API）
 * 需求：将所有数据存储到topic的第0号分区上
 */
public class CustomPartitionernew implements Partitioner {

    @Override
    public void configure(Map<String, ?> configs) {

    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        // 控制分区
        return 0;
    }

    @Override
    public void close() {

    }
}

