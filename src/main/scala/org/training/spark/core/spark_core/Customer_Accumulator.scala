package org.training.spark.core.spark_core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
  * 自定义累加器：
  * 功能：
  * 累加器用来对信息进行聚合。
  * 通常在向 Spark 传递函数时，比如使用 map() 函数或者用 filter() 传条件时，可以使用驱动器(driver)程序中定义的变量，
  * 但是集群中运行的每个任务都会得到这些变量的一份新的副本，更新这些副本的值也不会影响驱动器(driver)中的对应变量。
  * 如果我们想实现所有Partition处理时更新共享变量的功能，那么累加器可以实现我们想要的效果。
  */
class Customer_Accumulator extends AccumulatorV2[String, mutable.HashMap[String, Int]] {

    private val _hashAcc = new mutable.HashMap[String, Int]()

    // 检测是否为空
    override def isZero: Boolean = {
        _hashAcc.isEmpty
    }

    // 拷贝一个新的累加器
    override def copy(): AccumulatorV2[String, mutable.HashMap[String, Int]] = {

        val newAcc = new Customer_Accumulator()

        _hashAcc.synchronized {
            newAcc._hashAcc ++= (_hashAcc)
        }
        newAcc
    }

    // 重置一个累加器
    override def reset(): Unit = {
        _hashAcc.clear()
    }

    // 每一个分区中用于添加数据的方法 小SUM
    override def add(v: String): Unit = {

        _hashAcc.get(v) match {
            case None => _hashAcc += ((v, 1))
            case Some(a) => _hashAcc += ((v, a + 1))
        }

    }

    // 合并每一个分区的输出 总sum
    override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Int]]): Unit = {

        other match {
            case o: AccumulatorV2[String, mutable.HashMap[String, Int]] => {

                for ((k, v) <- o.value) {

                    _hashAcc.get(k) match {
                        case None => _hashAcc += ((k, v))
                        case Some(a) => _hashAcc += ((k, a + v))
                    }

                }

            }
        }

    }

    // 输出值
    override def value: mutable.HashMap[String, Int] = {
        _hashAcc
    }
}



object  Customer_Accumulator{
    def main(args: Array[String]): Unit = {
        val conf=new SparkConf().setAppName("Coustom_Accumulator").setMaster("local[*]")
        val sc=new SparkContext(conf)

        val hashAcc = new Customer_Accumulator()

        sc.register(hashAcc,"Coustom_Accumulator")
        val rdd = sc.makeRDD(Array("a","b","c","d","e","a","c"))

        rdd.foreach(hashAcc.add(_))
        hashAcc.value

        for((k,v)<-hashAcc.value){
            println("["+k+":"+v+"]")
        }

        sc.stop()
    }
}