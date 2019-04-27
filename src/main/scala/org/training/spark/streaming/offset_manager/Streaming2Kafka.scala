//package org.training.spark.streaming.offset_manager
//
//import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo, TopicMetadataRequest}
//import kafka.common.TopicAndPartition
//import kafka.consumer.SimpleConsumer
//import kafka.message.MessageAndMetadata
//import kafka.serializer.StringDecoder
//import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
//import org.I0Itec.zkclient.ZkClient
//import org.apache.kafka.clients.consumer.ConsumerConfig
//import org.apache.kafka.clients.producer.ProducerRecord
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming.dstream.InputDStream
//import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//
//object Streaming2Kafka {
//
//  def main(args: Array[String]): Unit = {
//
//    val sparkConf = new SparkConf().setAppName("kafak").setMaster("local[*]")
//
//    val ssc = new StreamingContext(sparkConf, Seconds(5))
//
//    val fromTopic = "x"
//    val toTopic = "y"
//
//    val brokers = "hadoop100:9092,hadoop101:9092,hadoop102:9092"
//    //获取zookeeper的信息
//    val zookeeper = "hadoop100:2181,hadoop101:2181,hadoop102:2181"
//
//    val kafkaPro = Map[String,String](
//      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,//用于初始化链接到集群的地址
//      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
//      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
//      //用于标识这个消费者属于哪个消费团体
//      ConsumerConfig.GROUP_ID_CONFIG -> "kafkaxy",
//      //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
//      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "largest"
//    )
//
//    //获取保存offset的zk路径   driver端
//    val topicDirs = new ZKGroupTopicDirs("kafkaxy",fromTopic)
//    val zkTopicPath = s"${topicDirs.consumerOffsetDir}"
//
//    //创建一个到Zk的连接
//    val zkClient = new ZkClient(zookeeper)
//
//    // 获取偏移的保存地址目录下的子节点
//    val children = zkClient.countChildren(zkTopicPath)
//
//    var stream:InputDStream[(String,String)] = null
//
//    // > 0 说明有过保存
//    if(children > 0){
//
//      // 新建一个变量，保存消费的偏移量
//      var fromOffsets:Map[TopicAndPartition, Long] = Map()
//
//      //首先获取每一个Partiton的主节点的信息
//      val topicList = List(fromTopic)
//      //创建一个获取元信息的请求
//      val request = new TopicMetadataRequest(topicList,0)
//      //创建了一个客户端 到Kafka的连接 kafka是会有过期时间的。
//      val getLeaderConsumer
//      = new SimpleConsumer("hadoop100",9092,100000,10000,"OffsetLookUp")
//
//      val response = getLeaderConsumer.send(request)
//
//      val topicMeteOption = response.topicsMetadata.headOption//这个topic的所有的元信息
//
//      //可能从kafka获取不到元信息
//      val parttitons = topicMeteOption match{
//        case Some(tm) => {
//          tm.partitionsMetadata.map(pm => (pm.partitionId,pm.leader.get.host)).toMap[Int,String]
//      }
//        case None => {
//          Map[Int,String]()
//        }
//      }
//      getLeaderConsumer.close()
//      println("parttions infomation is:"+parttitons)
//      println("children information is:"+children)
//
//      for (i <- 0 until children){ //children是分区的个数  对每个分区单独去处理
//
//        // 获取保存在ZK中的偏移信息
//        val partitionOffset = zkClient.readData[String](s"${topicDirs.consumerOffsetDir}/${i}")
//        println(s"Partition【${i}】 目前保存的偏移信息是：${partitionOffset}")
//
//        val tp = TopicAndPartition(fromTopic,i)
//
//        // 获取当前Parttiton的最小偏移值（主要防止Kafka中的数据过期问题）
//        val requesMin = OffsetRequest(Map(tp -> PartitionOffsetRequestInfo(OffsetRequest.EarliestTime,1)))
//        val consumerMin = new SimpleConsumer(parttitons(i),9092,100000,10000,
//          "getMiniOffset")
//        val response = consumerMin.getOffsetsBefore(requesMin)
//
//        // 获取当前的偏移量
//        val curOffsets = response.partitionErrorAndOffsets(tp).offsets
//        consumerMin.close()
//
//        var nextOffset = partitionOffset.toLong
//
//        if(curOffsets.length > 0 && curOffsets.head > nextOffset){
//          nextOffset = curOffsets.head
//        }
//        println(s"Partition【${i}】 最小的偏移信息是：${curOffsets.head}")
//        println(s"Partition【${i}】 修正后的偏移信息是：${nextOffset}")
//
//        fromOffsets += (tp -> nextOffset)
//      }
//
//      val messageHandler = (mmd: MessageAndMetadata[String,String]) => (mmd.topic, mmd.message())
//      println("从ZK获取偏移量来创建DStream")
//      zkClient.close()
//      stream = KafkaUtils
//          .createDirectStream[String,String,StringDecoder,StringDecoder,(String,String)](ssc,kafkaPro,fromOffsets,messageHandler)
//    }else{
//      println("直接创建，没有从ZK中获取偏移量")
//      stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaPro, Set(fromTopic))
//
//    }
//
//    var offsetRanges = Array[OffsetRange]()
//
//    // 获取采集的数据的偏移量
//    val mapDstream = stream.transform{ rdd =>
//      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//      rdd
//    }.map(_._2)
//
//    // 根据上一次的Offset来创建
//    // 链接到了kafka
//    //val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaPro, Set(fromTopic))
//
//    // 获取上一次的Offset
//
//    mapDstream.map("ABC:" + _ ).foreachRDD { rdd =>
//
//      rdd.foreachPartition { items =>
//
//        //写回kafka, 连接池
//        val kafkaProxyPool = KafkaPool(brokers)
//        val kafkaProxy = kafkaProxyPool.borrowObject()
//
//        for (item <- items) {
//          //使用
//          kafkaProxy.kafkaClient.send(new ProducerRecord[String, String](toTopic, item))
//        }
//
//        kafkaProxyPool.returnObject(kafkaProxy)
//      }
//
//      //更新Offset  在executor端的地址及目录
//      val updateTopicDirs = new ZKGroupTopicDirs("kafkaxy",fromTopic)
//      val updateZkClient = new ZkClient(zookeeper)
//      for(offset <- offsetRanges){
//        println(offset)
//        val zkPath = s"${updateTopicDirs.consumerOffsetDir}/${offset.partition}"
//        ZkUtils.updatePersistentPath(updateZkClient,zkPath,offset.fromOffset.toString)
//      }
//      updateZkClient.close()
//    }
//
//    ssc.start()
//    ssc.awaitTermination()
//
//  }
//
//}
