package org.training.spark.util

object KafkaRedisProperties {
  //127.0.0.1:6379
    //192.168.228.130:6379
  val REDIS_SERVER: String = "hadoop100"
  val REDIS_PORT: Int = 6379

//  val KAFKA_SERVER: String = "hadoop100"
  val KAFKA_ADDR: String = "hadoop100:9092,hadoop101:9092,hadoop102:9092"
  val KAFKA_USER_TOPIC: String = "user_events"
  val KAFKA_RECO_TOPIC: String = "reco6"

}