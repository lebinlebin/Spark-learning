package org.training.spark.streaming.spark_streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by liulebin on 03/24/2019.
  */
object statefulWorldCount {

  def main(args: Array[String]) {
    // 需要创建一个SparkConf
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    // 需要创建一个StreamingContext
    val ssc = new StreamingContext(conf, Seconds(3))
    // 需要设置一个checkpoint的目录。
    ssc.checkpoint(".")

    // 通过StreamingContext来获取master01机器上9999端口传过来的语句
    val lines = ssc.socketTextStream("hadoop100", 9999)

    // 需要通过空格将语句中的单词进行分割DStream[RDD[String]]
    val words = lines.flatMap(_.split(" "))

    //import org.apache.spark.streaming.StreamingContext._ // not necessary since Spark 1.3
    // 需要将每一个单词都映射成为一个元组（word,1）
    val pairs = words.map(word => (word, 1))


    // 定义一个更新方法，values是当前批次RDD中相同key的value集合，state是框架提供的上次state的值

    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      // 计算当前批次相同key的单词总数
      val currentCount = values.foldLeft(0)(_ + _)
      // 获取上一次保存的单词总数
      val previousCount = state.getOrElse(0)
      // 返回新的单词总数
      Some(currentCount + previousCount)
    }

    // 使用updateStateByKey方法，类型参数是状态的类型，后面传入一个更新方法。
    val stateDstream = pairs.updateStateByKey[Int](updateFunc)
    //输出
    stateDstream.print()
//    stateDstream.saveAsTextFiles("hdfs://hadoop100:9000/statful/","abc")//指定前缀保存内容

    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate
    //ssc.stop()
  }

}
