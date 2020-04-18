package org.training.spark.featureTransform

// 参考自spark官网教程 http://spark.apache.org/docs/latest/ml-features.html#tf-idf
// In the following code segment, we start with a set of sentences.
// We split each sentence into words using Tokenizer. For each sentence (bag of words),
// we use HashingTF to hash the sentence into a feature vector. We use IDF to rescale
// the feature vectors; this generally improves performance when using text as features. // Our feature vectors could then be passed to a learning algorithm.
/**
2.15 MaxAbsScaler(绝对值规范化)
MaxAbsScaler使用每个特征的最大值的绝对值将输入向量的特征值（各特征值除以最大绝对值）转换到[-1,1]之间。因为它不会转移／集中数据，所以不会破坏数据的稀疏性。
下面的示例展示如果读入一个libsvm形式的数据以及调整其特征值到[-1,1]之间。
  */


import org.apache.spark.sql.SparkSession

// 创建实例数据

object MaxAbsScaler {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.
      builder().
      master("local[*]").
      appName("tf-idf").
      config("spark.debug.maxToStringFields","1000").
      //enableHiveSupport().
      getOrCreate()
    val sc = spark.sparkContext


    import org.apache.spark.ml.feature.MaxAbsScaler

    val dataFrame = spark.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")
    val scaler = new MaxAbsScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")

    // Compute summary statistics and generate MaxAbsScalerModel
    val scalerModel = scaler.fit(dataFrame)

    // rescale each feature to range [-1, 1]
    val scaledData = scalerModel.transform(dataFrame)
    scaledData.show()
    sc.stop()
    
  }


}
