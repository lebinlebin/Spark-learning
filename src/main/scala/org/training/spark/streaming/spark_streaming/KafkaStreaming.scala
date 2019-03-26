package org.training.spark.streaming.spark_streaming

import org.apache.commons.pool2.impl.{GenericObjectPool, GenericObjectPoolConfig}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.api.java.function.VoidFunction
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
/**
  * Created by liulebin on 03/10/2019.
  */

//单例对象 sparkStreaming.KafkaStreaming
object createKafkaProducerPool{

  //用于返回真正的对象池GenericObjectPool
  def apply(brokerList: String, topic: String):  GenericObjectPool[KafkaProducerProxy] = {
    val producerFactory = new BaseKafkaProducerFactory(brokerList, defaultTopic = Option(topic))
    val pooledProducerFactory = new PooledKafkaProducerAppFactory(producerFactory)
    //指定了你的kafka对象池的大小
    val poolConfig = {
      val c = new GenericObjectPoolConfig
      val maxNumProducers = 10
      c.setMaxTotal(maxNumProducers)
      c.setMaxIdle(maxNumProducers)
      c
    }
    //返回一个对象池
    new GenericObjectPool[KafkaProducerProxy](pooledProducerFactory, poolConfig)
  }
}

/*
bin/zkServer.sh start
bin/zkServer.sh status

[atguigu@hadoop102 kafka]$ bin/kafka-server-start.sh config/server.properties &
[atguigu@hadoop103 kafka]$ bin/kafka-server-start.sh config/server.properties &
[atguigu@hadoop104 kafka]$ bin/kafka-server-start.sh config/server.properties &

kafka-console-producer.sh --broker-list hadoop100:9092,hadoop101:9092,hadoop102:9092 --topic source
kafka-console-consumer.sh --broker-list hadoop100:9092,hadoop101:9092,hadoop102:9092 --topic target
bin/kafka-console-consumer.sh --zookeeper hadoop100:2181 --from-beginning --topic source

 */
object KafkaStreaming{

  def main(args: Array[String]) {

    //设置sparkconf
    val conf = new SparkConf().setMaster("local[4]").setAppName("NetworkWordCount")
    //新建了streamingContext
    val ssc = new StreamingContext(conf, Seconds(1))

    //kafka的地址
    val brobrokers = "192.168.228.130:9092,192.168.228.131:9092,192.168.228.130:9092"
    //kafka的队列名称
    val sourcetopic="source"

    //kafka的队列名称
    val targettopic="target"

    //创建消费者组名
    var group="con-consumer-group"

    //kafka消费者配置
    val kafkaParam = Map(
      "bootstrap.servers" -> brobrokers,//用于初始化链接到集群的地址
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      //用于标识这个消费者属于哪个消费团体
      "group.id" -> group,
      //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
      //可以使用这个配置，latest自动重置偏移量为最新的偏移量
      "auto.offset.reset" -> "latest",
      //如果是true，则这个消费者的偏移量会在后台自动提交
      "enable.auto.commit" -> (false: java.lang.Boolean)
      //ConsumerConfig.GROUP_ID_CONFIG
    )

    //创建DStream，返回接收到的输入数据
    var stream=KafkaUtils.
        createDirectStream[String,String](ssc, LocationStrategies.PreferConsistent,ConsumerStrategies.Subscribe[String,String](Array(sourcetopic),kafkaParam))


    //每一个stream都是一个ConsumerRecord
    stream.map(s =>("id:" + s.key(),">>>>:"+s.value())).foreachRDD(rdd => {
      //对于RDD的每一个分区执行一个操作
      rdd.foreachPartition(partitionOfRecords => {
        // kafka连接池。
        val pool = createKafkaProducerPool(brobrokers, targettopic)
        //从连接池里面取出了一个Kafka的连接
        val p = pool.borrowObject()
        //发送当前分区里面每一个数据到target topic
        partitionOfRecords.foreach {message => System.out.println(message._2);
          p.send(message._2,Option(targettopic))}

        // 使用完了需要将kafka还回去
        pool.returnObject(p)

      })

      //更新offset信息
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges


    })

    ssc.start()
    ssc.awaitTermination()

  }
}
