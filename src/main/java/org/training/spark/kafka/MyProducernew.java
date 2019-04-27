package org.training.spark.kafka;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 创建生产者（新API）
 */
public class MyProducernew {

    //消费指定主题，指定分区，指定偏移量数据
    public static void main(String[] args) {

        //kafka集群
        ArrayList<String> brokers = new ArrayList<>();
        brokers.add("hadoop100");
        brokers.add("hadoop101");
        brokers.add("hadoop102");
        //端口号
        int port = 9092;
        //主题
        String topic = "second";
        //分区号
        int partition = 0;
        //偏移量
        long offset = 15;

        MyProducernew mySimpleConsumer = new MyProducernew();
        mySimpleConsumer.getData(brokers, port, topic, partition, offset);
    }

    public void getData(List<String> brokers, int port, String topic, int partition, long offset) {

        PartitionMetadata partitionMetadata = getLeader(brokers, port, topic, partition);

        //获取指定分区的leader（String）
        String leader = partitionMetadata.leader().host();

        //获取consumer对象
        SimpleConsumer consumer = new SimpleConsumer(leader, port, 1000, 1024 * 4, "client" + topic);

        //拉取数据请求
        FetchRequest fetchRequest = new FetchRequestBuilder().clientId("client" + topic).addFetch(topic, partition, offset, 1000).build();
        //拉取数据
        FetchResponse fetchResponse = consumer.fetch(fetchRequest);

        ByteBufferMessageSet messageAndOffsets = fetchResponse.messageSet(topic, partition);

        //打印数据信息
        for (MessageAndOffset messageAndOffset : messageAndOffsets) {
            String s = String.valueOf(messageAndOffset.message().payload().get());
            System.out.println("offset:" + messageAndOffset.offset()
                    + "---" + s);
        }

        consumer.close();
    }

    //获取分区leader
    public PartitionMetadata getLeader(List<String> brokers, int port, String topic, int partition) {
        SimpleConsumer consumer = null;
        for (String broker : brokers) {
            consumer = new SimpleConsumer(broker, port, 1000, 1024 * 4, "client");

            TopicMetadataRequest topicMetadataRequest = new TopicMetadataRequest(Collections.singletonList(topic));
            //获取topic元数据信息
            TopicMetadataResponse topicMetadataResponse = consumer.send(topicMetadataRequest);

            List<TopicMetadata> topicMetadata = topicMetadataResponse.topicsMetadata();

            for (TopicMetadata topicMetadatum : topicMetadata) {
                //获取一个topic中所有的分区元数据信息
                List<PartitionMetadata> partitionMetadata = topicMetadatum.partitionsMetadata();
                for (PartitionMetadata partitionMetadatum : partitionMetadata) {
                    if (partitionMetadatum.partitionId() == partition) {
                        consumer.close();
                        return partitionMetadatum;
                    }
                }
            }
        }
        assert consumer != null;
        consumer.close();
        return null;
    }
}
