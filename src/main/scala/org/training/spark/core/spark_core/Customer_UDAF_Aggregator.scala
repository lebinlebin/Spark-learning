package org.training.spark.core.spark_core

import org.apache.spark.sql.SparkSession
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
一、UDAF简介
2.2 继承Aggregator
还有另一种方式就是继承Aggregator这个类，优点是可以带类型：
  */
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders}

/**
  * 计算平均值
  *
  */
object AverageAggregator extends Aggregator[User, Average, Double] {

    // 初始化buffer
    override def zero: Average = Average(0L, 0L)

    // 处理一条新的记录
    override def reduce(b: Average, a: User): Average = {
        b.sum += a.age
        b.count += 1L
        b
    }

    // 合并聚合buffer
    override def merge(b1: Average, b2: Average): Average = {
        b1.sum += b2.sum
        b1.count += b2.count
        b1
    }

    // 减少中间数据传输
    override def finish(reduction: Average): Double = reduction.sum.toDouble / reduction.count

    override def bufferEncoder: Encoder[Average] = Encoders.product

    // 最终输出结果的类型
    override def outputEncoder: Encoder[Double] = Encoders.scalaDouble

}



/**
  * 计算平均值过程中使用的Buffer
  *
  * @param sum
  * @param count
  */
case class Average(var sum: Long, var count: Long) {
}

case class User(id: Long, name: String, sex: String, age: Long) {
}



object AverageAggregatorDemo {

    def main(args: Array[String]): Unit = {

        val spark = SparkSession.builder().master("local[*]").appName("SparkStudy").getOrCreate()
        import spark.implicits._
        val user = spark.read.json("data/user/user.json").as[User]
        user.select(AverageAggregator.toColumn.name("avg")).show()

    }

}




