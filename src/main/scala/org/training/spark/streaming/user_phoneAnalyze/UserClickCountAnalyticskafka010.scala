package org.training.spark.streaming.user_phoneAnalyze

import com.alibaba.fastjson.JSON
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.training.spark.util.{KafkaRedisProperties, RedisClient}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010._

object UserClickCountAnalyticskafka010 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UserClickCountAnalytics")
    if (args.length == 0) {
      conf.setMaster("local[*]")
    }

    val ssc = new StreamingContext(conf, Seconds(5))
    val clickHashKey = "app::users::click"


    /**
      * kafka 0.8的写法
      */
//    // Kafka configurations
//    val topics08 = KafkaRedisProperties.KAFKA_USER_TOPIC.split("\\,").toSet
//    println(s"Topics: ${topics08}")
//
//    val brokers08 = KafkaRedisProperties.KAFKA_ADDR
//    val kafkaParams = Map[String, String](
//      "metadata.broker.list" -> brokers08,
//      "serializer.class" -> "kafka.serializer.StringEncoder"
//    )
//
//    val clickHashKey = "app::users::click"
//
//    // Create a direct stream
//    val kafkaStream = KafkaUtils.createDirectStream[String, String](
//      ssc,
//      PreferConsistent,
//      Subscribe[String, String](topics08, kafkaParams)
//      )


    /**
      * kafka1.0的写法
      */

    // 设置检查点目录
    ssc.checkpoint("./streaming_checkpoint")

    // 获取Kafka配置
    val broker_list = "hadoop100:9092,hadoop101:9092,hadoop102:9092"
    val topics = "user_events"

    // kafka消费者配置
    val kafkaParam010 = Map(
      "bootstrap.servers" -> broker_list, //用于初始化链接到集群的地址
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      //用于标识这个消费者属于哪个消费团体
      "group.id" -> "lebin",
      //commerce-consumer-group
      //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
      //可以使用这个配置，latest自动重置偏移量为最新的偏移量
      // auto.offset.reset
      // latest: 先去Zookeeper获取offset，如果有，直接使用，如果没有，从最新的数据开始消费；
      // earlist: 先去Zookeeper获取offset，如果有，直接使用，如果没有，从最开始的数据开始消费
      // none: 先去Zookeeper获取offset，如果有，直接使用，如果没有，直接报错
      "auto.offset.reset" -> "latest",
      //如果是true，则这个消费者的偏移量会在后台自动提交
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    // 创建DStream，返回接收到的输入数据
    // LocationStrategies：根据给定的主题和集群地址创建consumer
    // LocationStrategies.PreferConsistent：持续的在所有Executor之间分配分区
    // ConsumerStrategies：选择如何在Driver和Executor上创建和配置Kafka Consumer
    // ConsumerStrategies.Subscribe：订阅一系列主题
    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topics), kafkaParam010)
    )
//    kafkaStream.print(100)


    /**
      * 对topic中的数据进行实时的处理
      */
    val events = kafkaStream.flatMap(line => {
      println(s"Line ${line.value()}.")
      val data = JSON.parseObject(line.value())
      Some(data)
    })

/*
       key   value
Line (null,{"uid":"c8ee90aade1671a21336c721512b817a","os_type":"Android","click_count":9,"event_time":"1552292175544"}).
Line (null,{"uid":"6b67c8c700427dee7552f81f3228c927","os_type":"Android","click_count":3,"event_time":"1552292175745"}).
Line (null,{"uid":"a95f22eabc4fd4b580c011a3161a9d9d","os_type":"Android","click_count":5,"event_time":"1552292175945"}).
*/
//    // Compute user click times
    val userClicks = events.map( x => ( x.getString("uid"), x.getLong("click_count"))).reduceByKey(_ + _)

    userClicks.foreachRDD( rdd => {
      rdd.foreachPartition( partitionOfRecords => {
        val jedis = RedisClient.pool.getResource
          jedis.auth("123456")
          partitionOfRecords.foreach( pair => {
          try {
            val uid = pair._1
            val clickCount = pair._2
            jedis.hincrBy(clickHashKey, uid, clickCount)
            /*
HINCRBY key field increment
为哈希表 key 中的域 field 的值加上增量 increment 。
增量也可以为负数，相当于对给定域进行减法操作。
如果 key 不存在，一个新的哈希表被创建并执行 HINCRBY 命令。
如果域 field 不存在，那么在执行命令前，域的值被初始化为 0 。
对一个储存字符串值的域 field 执行 HINCRBY 命令将造成一个错误。
本操作的值被限制在 64 位(bit)有符号数字表示之内。
             */
            println(s"Update uid ${uid} to ${clickCount}.")
          } catch {
            case e: Exception => println("error:" + e)
          }
        })

        // destroy jedis object, please notice pool.returnResource is deprecated
        jedis.close()
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
