package org.training.spark.streaming.spark_streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by liulebin on 03/10/2019.
  */
object WordCount {
  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(1))

    // Create a DStream that will connect to hostname:port, like localhost:9999
    val lines = ssc.socketTextStream("hadoop100", 9999)

    // Split each line into words
    val words = lines.flatMap(_.split(" "))

    //import org.apache.spark.streaming.StreamingContext._ // not necessary since Spark 1.3
    // Count each word in each batch
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    // Print the first ten elements of each RDD generated in this DStream to the console
    wordCounts.print()

    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate

    //ssc.stop()
  }
}
