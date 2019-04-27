package org.training.spark.kafkaProducer;

import kafka.producer.Partitioner;

/**
 * 自定义分区生产者
 * 0）需求：将所有数据存储到topic的第0号分区上
 * 1）定义一个类实现Partitioner接口，重写里面的方法（过时API）
 */
public class CustomPartitionerold implements Partitioner {

    public CustomPartitionerold() {
        super();
    }

    @Override
    public int partition(Object key, int numPartitions) {
        // 控制分区
        return 0;
    }
}
