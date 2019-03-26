package org.training.spark.core.spark_core

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
  * 将相同后缀的数据分区到相同的分区并保存输出来实现。
  */
class CustomerPartitioner(numParts:Int) extends Partitioner {

  //覆盖分区数
  override def numPartitions: Int = numParts

  //覆盖分区号获取函数
  override def getPartition(key: Any): Int = {
    val ckey: String = key.toString
    ckey.substring(ckey.length-1).toInt%numParts
  }
}
/*
总结：substring(x)是从字符串的的第x个字符截取
     substring(x,y）是从x到y前的位置停止
 */

object CustomerPartitioner extends App{
  val conf=new SparkConf()
      .setAppName("partitioner")
      .setMaster("local[*]")
  val sc=new SparkContext(conf)

  val data=sc.parallelize(List("aa.2","bb.2","cc.3","dd.3","ee.5"))

  val result = data.map((_,1)).partitionBy(new CustomerPartitioner(5))

  result.mapPartitionsWithIndex((index,items)=>Iterator(index+":"+items.mkString("|"))).collect().foreach(println)
    //.keys.saveAsTextFile("hdfs://master01:9000/partitioner")

  sc.stop()
}


