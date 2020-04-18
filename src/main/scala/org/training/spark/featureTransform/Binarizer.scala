package org.training.spark.featureTransform

// 参考自spark官网教程 http://spark.apache.org/docs/latest/ml-features.html#tf-idf
// In the following code segment, we start with a set of sentences.
// We split each sentence into words using Tokenizer. For each sentence (bag of words),
// we use HashingTF to hash the sentence into a feature vector. We use IDF to rescale
// the feature vectors; this generally improves performance when using text as features. // Our feature vectors could then be passed to a learning algorithm.
/**
二元化（Binarization）是通过（选定的）阈值将数值化的特征转换成二进制（0/1）特征表示的过程。
Binarizer（ML提供的二元化方法）二元化涉及的参数有inputCol（输入）、outputCol（输出）以及threshold（阀值）。
（输入的）特征值大于阀值将映射为1.0，特征值小于等于阀值将映射为0.0。（Binarizer）支持向量（Vector）和双精度（Double）类型的输出
  */


import org.apache.spark.sql.SparkSession

// 创建实例数据

object Binarizer {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.
      builder().
      master("local[*]").
      appName("tf-idf").
      config("spark.debug.maxToStringFields","1000").
      //enableHiveSupport().
      getOrCreate()
    val sc = spark.sparkContext

    import org.apache.spark.ml.feature.Binarizer

    val data = Array((0, 0.1), (1, 0.8), (2, 0.2))
    val dataFrame = spark.createDataFrame(data).toDF("label", "feature")

    val binarizer: Binarizer = new Binarizer()
      .setInputCol("feature")
      .setOutputCol("binarized_feature")
      .setThreshold(0.5)

    val binarizedDataFrame = binarizer.transform(dataFrame)
    val binarizedFeatures = binarizedDataFrame.select("binarized_feature")
    binarizedFeatures.collect().foreach(println)

    sc.stop()
    
  }


}
