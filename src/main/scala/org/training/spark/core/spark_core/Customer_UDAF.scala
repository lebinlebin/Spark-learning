package org.training.spark.core.spark_core

import org.apache.spark.sql.SparkSession
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
一、UDAF简介
先解释一下什么是UDAF（User Defined Aggregate Function），即用户定义的聚合函数，聚合函数和普通函数的区别是什么呢，普通函数是接受一行输入产生一个输出，聚合函数是接受一组（一般是多行）输入然后产生一个输出，即将一组的值想办法聚合一下。
关于UDAF的一个误区
我们可能下意识的认为UDAF是需要和group by一起使用的，实际上UDAF可以跟group by一起使用，也可以不跟group by一起使用，这个其实比较好理解，联想到mysql中的max、min等函数，可以:
	select max(foo) from foobar group by bar;
表示根据bar字段分组，然后求每个分组的最大值，这时候的分组有很多个，使用这个函数对每个分组进行处理，也可以：
	select max(foo) from foobar;
这种情况可以将整张表看做是一个分组，然后在这个分组（实际上就是一整张表）中求最大值。所以聚合函数实际上是对分组做处理，而不关心分组中记录的具体数量。
  */
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

object Customer_UDAF extends UserDefinedAggregateFunction {

    // 聚合函数的输入数据结构
    override def inputSchema: StructType = StructType(StructField("input", LongType) :: Nil)

    // 缓存区数据结构
    override def bufferSchema: StructType = StructType(StructField("sum", LongType) :: StructField("count", LongType) :: Nil)

    // 聚合函数返回值数据结构
    override def dataType: DataType = DoubleType

    // 聚合函数是否是幂等的，即相同输入是否总是能得到相同输出
    override def deterministic: Boolean = true

    // 初始化缓冲区
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
        buffer(0) = 0L
        buffer(1) = 0L
    }

    // 给聚合函数传入一条新数据进行处理
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        if (input.isNullAt(0)) return
        buffer(0) = buffer.getLong(0) + input.getLong(0)
        buffer(1) = buffer.getLong(1) + 1
    }

    // 合并聚合函数缓冲区
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
        buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
        buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
    }

    // 计算最终结果
    override def evaluate(buffer: Row): Any = buffer.getLong(0).toDouble / buffer.getLong(1)

}

object Customer_UDAFmain {

    def main(args: Array[String]): Unit = {

        val spark = SparkSession.builder().master("local[*]").appName("SparkStudy").getOrCreate()
        spark.read.json("data/user/user.json").createOrReplaceTempView("v_user")
        spark.udf.register("u_avg", Customer_UDAF)
        // 将整张表看做是一个分组对求所有人的平均年龄
        spark.sql("select count(1) as count, u_avg(age) as avg_age from v_user").show()
        // 按照性别分组求平均年龄
        spark.sql("select sex, count(1) as count, u_avg(age) as avg_age from v_user group by sex").show()

    }

}



