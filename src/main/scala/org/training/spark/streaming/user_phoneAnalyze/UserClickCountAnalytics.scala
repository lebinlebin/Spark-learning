package org.training.spark.streaming.user_phoneAnalyze

import com.alibaba.fastjson.JSON
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.training.spark.util.{KafkaRedisProperties, RedisClient}

object UserClickCountAnalytics {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UserClickCountAnalytics")
    if (args.length == 0) {
      conf.setMaster("local[*]")
    }

    val ssc = new StreamingContext(conf, Seconds(5))

    // Kafka configurations
    val topics = KafkaRedisProperties.KAFKA_USER_TOPIC.split("\\,").toSet
    println(s"Topics: ${topics}.")

    val brokers = KafkaRedisProperties.KAFKA_ADDR
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "serializer.class" -> "kafka.serializer.StringEncoder"
    )

    val clickHashKey = "app::users::click"

    // Create a direct stream
    val kafkaStream = KafkaUtils
        .createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    val events = kafkaStream.flatMap(line => {
      println(s"Line ${line}.")
      val data = JSON.parseObject(line._2)
      Some(data)
    })

/*
Line (null,{"uid":"c8ee90aade1671a21336c721512b817a","os_type":"Android","click_count":9,"event_time":"1552292175544"}).
Line (null,{"uid":"6b67c8c700427dee7552f81f3228c927","os_type":"Android","click_count":3,"event_time":"1552292175745"}).
Line (null,{"uid":"a95f22eabc4fd4b580c011a3161a9d9d","os_type":"Android","click_count":5,"event_time":"1552292175945"}).
*/
    // Compute user click times
    val userClicks = events.map(x => (x.getString("uid"), x.getLong("click_count"))).reduceByKey(_ + _)
    userClicks.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        val jedis = RedisClient.pool.getResource
          jedis.auth("123456")
          partitionOfRecords.foreach(pair => {
          try {
            val uid = pair._1
            val clickCount = pair._2
            jedis.hincrBy(clickHashKey, uid, clickCount)
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
