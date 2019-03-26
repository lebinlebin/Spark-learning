package org.training.spark.streaming.spark_streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by liulebin on 03/24/2019.
  */
object window_WorldCount {

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    // batchInterval = 3s
    val ssc = new StreamingContext(conf, Seconds(2))
    ssc.checkpoint("./checkpoint")

    // Create a DStream that will connect to hostname:port, like localhost:9999
    val lines = ssc.socketTextStream("hadoop100", 9999)

    // Split each line into words
    val words = lines.flatMap(_.split(" "))

    //import org.apache.spark.streaming.StreamingContext._ // not necessary since Spark 1.3
    // Count each word in each batch
    val pairs = words.map(word => (word, 1))

    //val wordCounts = pairs.reduceByKey((a:Int,b:Int) => (a + b))

    // 窗口大小 为12s， 12/3 = 4  滑动步长 6S，   6/3 =2
    //val wordCounts = pairs.reduceByKeyAndWindow((a:Int,b:Int) => (a + b),Seconds(12), Seconds(6))

    val wordCounts2 = pairs.reduceByKeyAndWindow(_ + _,_ - _ ,Seconds(12), Seconds(6))

    // Print the first ten elements of each RDD generated in this DStream to the console
    wordCounts2.print()

    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate
    //ssc.stop()

  }

}
