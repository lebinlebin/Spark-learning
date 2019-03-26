package org.training.spark.core.spark_core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object share_varibles {

        def main(args:Array[String]) {
            val conf = new SparkConf().setAppName("july_online").setMaster("local[2]")
            val sc = new SparkContext(conf)
            sc.setLogLevel("WARN")
            val testRDD = sc.parallelize(Array(1, 2, 3))
            val broadCast = sc.broadcast(1)
            testRDD.map(_ + broadCast.value).foreach(println)
            val accumulator = sc.accumulator(4, "july_online")
            testRDD.foreach(accumulator += _)
            println(accumulator.value)
            //wrongmethod
            //testRDD.foreach(l=>println(l+accumulator.value))


            val bigRDD: RDD[(String, Long)] = sc.makeRDD(Array("spark", "hadoop", "hive", "yarn", "hbase", "flink", "flume", "kafka"), 4).zipWithIndex()
            val smallRDD = sc.makeRDD(Array("spark", "hadoop"), 4).zipWithIndex()
            val resultRDD1 = bigRDD.join(smallRDD)//相同的key组成过一个，value组成在一起
            //但是一个大的rdd和一个小的rdd，即，一个大表join一张小表，会产生shufle操作，里面的key会发上磁盘io和网络io
            //通过广播变量来解决这个问题
            resultRDD1.foreach(println)
            println()

            //将小的rdd放在广播变量里面
            val smallValue = smallRDD.collectAsMap()//小表可能只有几百兆。  这里smallRDD.collectAsMap()，发送到driver端，不是rdd了
            val smallBD = sc.broadcast(smallValue)
            // 通过 .mapPartitions  如果用join操作，会很耗时
            val resultRDD2 = bigRDD.mapPartitions { iter =>
            val small = smallBD.value//map变量   这个partition于小表数据进行join
            val arrayBuffer = ArrayBuffer[(String, Long, Long)]()
                iter.foreach { case (className, number) => {
                    if (small.contains(className)) {
                        arrayBuffer.+=((className, small.getOrElse(className, 0), number))
                    }
                    arrayBuffer.iterator//mapPartitions一定要有返回的类型
                }
                }
                arrayBuffer.iterator
            }
            resultRDD1.foreach(println)

            sc.stop()
        }
}
