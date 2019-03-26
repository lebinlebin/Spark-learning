package org.training.spark.streaming.spark_streaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by liulebin on 03/10/2019.
  */
// String就是接收数据的类型
class CustomReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) {
  override def onStart(): Unit = {
    // Start the thread that receives data over a connection
    new Thread("Socket Receiver") {
      override def run() { receive() }
    }.start()
  }

  override def onStop(): Unit = {
    // There is nothing much to do as the thread calling receive()
    // is designed to stop by itself if isStopped() returns false
  }

  /** Create a socket connection and receive data until receiver is stopped */
  private def receive() {
    var socket: Socket = null
    var userInput: String = null
    try {
      // Connect to host:port
      socket = new Socket(host, port)

      // Until stopped or connection broken continue reading
      val reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))

      userInput = reader.readLine()
      while(!isStopped && userInput != null) {

        // 內部的函數，將數據存儲下倆
        store(userInput)
        userInput = reader.readLine()
      }
      reader.close()
      socket.close()

      // Restart in an attempt to connect again when server is active again
      restart("Trying to connect again")
    } catch {
      case e: java.net.ConnectException =>
        // restart if could not connect to server
        restart("Error connecting to " + host + ":" + port, e)
      case t: Throwable =>
        // restart if there is any other error
        restart("Error receiving data", t)
    }
  }
}

object CustomReceiver {
  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local[*]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(1))

    // Create a DStream that will connect to hostname:port, like localhost:9999
    val lines = ssc.receiverStream(new CustomReceiver("hadoop100", 9999))

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