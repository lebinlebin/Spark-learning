package org.training.spark.TextProcessing

// 参考自spark官网教程 http://spark.apache.org/docs/latest/ml-features.html#tf-idf
// In the following code segment, we start with a set of sentences.
// We split each sentence into words using Tokenizer. For each sentence (bag of words),
// we use HashingTF to hash the sentence into a feature vector. We use IDF to rescale
// the feature vectors; this generally improves performance when using text as features. // Our feature vectors could then be passed to a learning algorithm.

import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
// 创建实例数据

object textTfIdfmllib {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.
      builder().
      appName("esc_data_processing").
      config("spark.debug.maxToStringFields","1000").
      enableHiveSupport().
      getOrCreate()
    val sc = spark.sparkContext


    /**
      * 2.基于RDD的MLlib包中的TF_IDF算法
      */

    //参考: http://spark.apache.org/docs/1.4.1/mllib-feature-extraction.html#tf-idfark.mllib.feature.HashingTF
    //进阶参考
    //http://blog.csdn.net/jiangpeng59/article/details/52786344


    // Load documents (one per line).
    val documents: RDD[Seq[String]] = sc.textFile("...").map(_.split(" ").toSeq)

    val hashingTF = new HashingTF()
//    val tf: RDD[Vector] = hashingTF.transform(documents)
//    tf.cache()
//    val idf = new IDF().fit(tf)
//    val tfidf: RDD[Vector] = idf.transform(tf)
    
  }


}
