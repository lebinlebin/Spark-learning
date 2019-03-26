package org.training.spark.core.spark_core

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.JavaConversions._

/**
  * 自定义累加器：
  * 功能：
  * 实现自定义类型累加器需要继承AccumulatorV2并至少覆写下例中出现的方法，
  * 下面这个累加器可以用于在程序运行过程中收集一些文本类信息，最终以Set[String]的形式返回。
  */
class LogAccumulator extends AccumulatorV2[String, java.util.Set[String]] {

    // 定义一个累加器的内存结构，用于保存带有字母的字符串。
    private val _logArray: java.util.Set[String] = new java.util.HashSet[String]()

    // 重写方法检测累加器内部数据结构是否为空。
    override def isZero: Boolean = {
        //检查_logArray 是否为空
        _logArray.isEmpty
    }

    // 重置你的累加器数据结构
    override def reset(): Unit = {
        //clear方法清空_logArray的所有内容
        _logArray.clear()
    }

    // 提供转换或者行动操作中添加累加器值的方法
    override def add(v: String): Unit = {
        // 将带有字母的字符串添加到_logArray内存结构中
        _logArray.add(v)
    }

    // 提供将多个分区的累加器的值进行合并的操作函数
    override def merge(other: AccumulatorV2[String, java.util.Set[String]]): Unit = {
        // 通过类型检测将o这个累加器的值加入到当前_logArray结构中
        other match {
            case o: LogAccumulator => _logArray.addAll(o.value)
        }

    }

    // 输出我的value值
    override def value: java.util.Set[String] = {
        java.util.Collections.unmodifiableSet(_logArray)
    }

    override def copy(): AccumulatorV2[String, java.util.Set[String]] = {
        val newAcc = new LogAccumulator()
        _logArray.synchronized{
            newAcc._logArray.addAll(_logArray)
        }
        newAcc
    }
}

// 过滤掉带字母的
object LogAccumulator {
    def main(args: Array[String]) {
        val conf=new SparkConf().setAppName("LogAccumulator").setMaster("local[*]")
        val sc=new SparkContext(conf)

        val accum = new LogAccumulator
        sc.register(accum, "logAccum")
        //如何在数据中应用正则表达式选择自己想要的格式的数据
        val sum = sc.parallelize(Array("1", "2a", "3", "4b", "5", "6", "7cd", "8", "9"), 2)
            .filter(line => {
            val pattern = """^-?(\d+)"""    //表示前面可以有一个可选的负号“-”开头，后面是一串数字组成的字符串
            val flag = line.matches(pattern)  //判断是否匹配
            if (!flag) {
                accum.add(line)  //不匹配的加入累加器，这里是留下了非全部由数字组成的字符串 "2a", "4b", "7cd"
            }
            flag //留下的数字，进行下一步操作，累加  1+3+5+6+8+9=32
        }).map(_.toInt).reduce(_ + _)

        println("sum: " + sum)
        //累加器的功能是什么？？ 这个累加器可以用于在程序运行过程中收集一些文本类信息，最终以Set[String]的形式返回
        for (v <- accum.value) print(v + " ")


        sc.stop()
    }
}


/**
正则表达式什么意思/^(-?\d+)(\.\d+)?$/
这个正则就是匹配数字；
-?表示前面可以有一个可选的减号；
\d+表示一到多个数字，(-?\d+)这个表示整数部分；
(\.\d+)?表示一个小数点跟多个数字,?表示前面这部分是可选的，这部分匹配一个可选的小数部分；
^(\d)$就是0-9的任意一个数字；
^表示以...开头，\d表示0-9的数字，$表示以...结尾；
所以这个就是表示单个数字了。
 */