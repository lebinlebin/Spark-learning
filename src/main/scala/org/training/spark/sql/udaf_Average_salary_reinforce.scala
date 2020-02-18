package org.training.spark.sql

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}

/**
  * 为DSL风格
  * 学习用户自定义聚合函数  UDAF   每个partion 小聚合  然后再大聚合函数
  * 需求：求取平均工资
  * 通过继承UserDefinedAggregateFunction来实现用户自定义聚合函数。
  * 下面展示一个求平均工资的自定义聚合函数。
  * 强类型用户自定义聚合函数,不是通过注册一张表的形式，而是用DSL风格
  */
// 既然是强类型，可能有case类
case class Employee(name: String, salary: Long)
case class Average(var sum: Long, var count: Long)
//                                     IN     BUF     OUT
class MyAverage extends Aggregator[Employee, Average, Double] {
  // 定义一个数据结构，保存工资总数和工资总个数，初始都为0
    //初始化方法 初始化每一个分区中的 共享变量
  def zero: Average = Average(0L, 0L)

  // 聚合相同executor分片中的结果
    //每一个分区中的每一条数据聚合的时候需要调用该方法
  def reduce(buffer: Average, employee: Employee): Average = {
    buffer.sum += employee.salary
    buffer.count += 1
    buffer
  }

  // 聚合不同execute的结果
    // 将每一个分区的输出 合并 形成最后的数据
  def merge(b1: Average, b2: Average): Average = {
    b1.sum += b2.sum
    b1.count += b2.count
    b1
  }

  // 计算输出  给出计算结果
  def finish(reduction: Average): Double = reduction.sum.toDouble / reduction.count

  // 设定中间值类型的编码器，要转换成case类
  // Encoders.product是进行scala元组和case类转换的编码器
  // 主要用于对共享变量进行编码
  def bufferEncoder: Encoder[Average] = Encoders.product

  // 设定最终输出值的编码器
  // 主要用于将输出进行编码
  def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

object MyAverage{
  def main(args: Array[String]) {
    //创建SparkConf()并设置App名称

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local[4]")
      .getOrCreate()

    import spark.implicits._
    // For implicit conversions like converting RDDs to DataFrames
    val ds = spark.read.json("data/employees.json").as[Employee]
    ds.show()


    val averageSalary = new MyAverage().toColumn.name("average_salary")

    val result = ds.select(averageSalary)
    result.show()
/*
+--------------+
|average_salary|
+--------------+
|        3750.0|
+--------------+
 */
    spark.stop()
  }
}
