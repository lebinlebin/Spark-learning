package org.training.spark.util

import redis.clients.jedis.{JedisPool, JedisPoolConfig}

object RedisClient extends Serializable {
  private var MAX_IDLE: Int = 20
  private var TIMEOUT: Int = 100
  private var TEST_ON_BORROW: Boolean = false
  private var MAX_Active: Int = 300


    lazy val config: JedisPoolConfig = {
    val config = new JedisPoolConfig
    config.setMaxIdle(MAX_IDLE)
    config.setTestOnBorrow(TEST_ON_BORROW)
    config.setMaxTotal(MAX_Active)
    config
  }



  lazy val pool = new JedisPool(config, KafkaRedisProperties.REDIS_SERVER,KafkaRedisProperties.REDIS_PORT, TIMEOUT)



  lazy val hook = new Thread {
    override def run = {
      println("Execute hook thread: " + this)
      pool.destroy()
    }
  }
  sys.addShutdownHook(hook.run)

    def main(args: Array[String]): Unit = {

        println(config.toString)
        println(KafkaRedisProperties.REDIS_SERVER)
        println(KafkaRedisProperties.REDIS_PORT)
        println(TIMEOUT)
    }
}