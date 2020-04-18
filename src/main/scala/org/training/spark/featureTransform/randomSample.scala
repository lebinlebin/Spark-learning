package org.training.spark.featureTransform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random
import org.apache.spark.sql.SparkSession

/**
 * sample(withReplacement, fraction, seed)采样
 *
 * big_xiao
 * 0.223
 * 2019.07.30 20:25:18
 * 字数 107
 * 阅读 543
 * sample(是否放回, fraction, seed)
 *
 * withReplacement：true抽取放回，false抽取不放回。
 *
 * fraction：
 * 1）false抽取不放回的情况下，抽取的概率（0-1）。
 * 0-全不抽
 * 1-全抽
 * 2）true抽取放回的情况下，抽取的次数。
 *
 *
 */
object randomSample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.
      builder().
      master("local[*]").
      appName("tf-idf").
      config("spark.debug.maxToStringFields","1000").
      //enableHiveSupport().
      getOrCreate()
    val sc = spark.sparkContext


    //seed：随机数种子。
    val rdd: RDD[Int] = sc.makeRDD(1 to 10)

    val rdd1: RDD[Int] = rdd.sample(true,2)

    val rdd2: RDD[Int] = rdd.sample(false,0.5)
    val rdd5: RDD[Int] = rdd.sample(false,0.5)

    val rdd3: RDD[Int] = rdd.sample(false,0.5,2)
    val rdd4: RDD[Int] = rdd.sample(false,0.5,2)

    println("----------原始数据----------------")
    rdd.collect().foreach(println)
    println("-----------有放回抽样,不设定随机种子,比例0.5---------------")
    rdd1.collect().foreach(println)

    println("****不放回抽样，不设定随机种子,比例0.5****")
    rdd2.collect().foreach(println)
    println("****不放回抽样，不设定随机种子,比例0.5****")
    rdd5.collect().foreach(println)

    println("****不放回抽样，设定随机种子，比例0.5****")
    rdd3.collect().foreach(println)
    println("****不放回抽样，设定随机种子，比例0.5****")
    rdd4.collect().foreach(println)


    println("*******随机数种子固定之后，随机数也就变成了固定的一些数字。*******")

    //随机数种子固定之后，随机数也就变成了固定的一些数字。
    val r1 = new Random(1000)
    for(i <- 1 to 10) {
      println(r1.nextInt(1000))

    }

    println("**************")

    val r2 = new Random(1000)
    for(i <- 1 to 10) {
      println(r2.nextInt(1000))
    }


    sc.stop()
    
  }


}
