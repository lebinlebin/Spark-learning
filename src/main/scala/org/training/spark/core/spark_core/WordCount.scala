package org.training.spark.core.spark_core

import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
  * Created by wuyufei on 31/07/2017.
  */
object WordCount {

  val logger = LoggerFactory.getLogger(WordCount.getClass)

  def main(args: Array[String]) {
    //创建SparkConf()并设置App名称
    /*val conf = new SparkConf().setMaster("spark://master01:7077").setAppName("WordCount")
        .set("spark.executor.cores","1")
        .setJars(List("C:\\Users\\Administrator\\Desktop\\Spark\\3.code\\spark\\sparkcore_wordcount\\target\\wordcount-jar-with-dependencies.jar"))
        .setIfMissing("spark.driver.host", "192.168.56.1")*/

    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建SparkContext，该对象是提交spark App的入口
    val sc = new SparkContext(conf)
    //使用sc创建RDD并执行相应的transformation和action
    val result = sc.textFile("hdfs://README").flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_, 1).sortBy(_._2, false)
    //停止sc，结束该任务

    result.collect().foreach(println _)


    result.saveAsTextFile("hdfs")



    logger.info("complete!")

    sc.stop()

  }

}