package org.training.spark.featureTransform

// 参考自spark官网教程 http://spark.apache.org/docs/latest/ml-features.html#tf-idf
// In the following code segment, we start with a set of sentences.
// We split each sentence into words using Tokenizer. For each sentence (bag of words),
// we use HashingTF to hash the sentence into a feature vector. We use IDF to rescale
// the feature vectors; this generally improves performance when using text as features. // Our feature vectors could then be passed to a learning algorithm.
/**
 * 2.14 MinMaxScaler（最大-最小规范化）
MinMaxScaler将所有特征向量线性变换到指定范围（最小-最大值）之间（归一化到[min, max]，通常为[0,1]）。它的参数有：
1. min：默认为0.0，为转换后所有特征的下边界。
2. max：默认为1.0，为转换后所有特征的上边界。
MinMaxScaler根据数据集的汇总统计产生一个MinMaxScalerModel。在计算时，该模型将特征向量一个一个分开计算并转换到指定的范围内的。
对于特征E来说，调整后的特征值如下：
Rescaled(ei)=ei−EminEmax−Emin∗(max−min)+min
如果Emax=Emin，Rescaled=0.5∗(max−min)。
注意：（1）最大最小值可能受到离群值的左右。（2）零值可能会转换成一个非零值，因此稀疏矩阵将变成一个稠密矩阵。
下面的示例展示如何读入一个libsvm形式的数据以及调整其特征值到[0,1]之间。
  */


import org.apache.spark.sql.SparkSession

// 创建实例数据

object MinMaxScaler {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.
      builder().
      master("local[*]").
      appName("tf-idf").
      config("spark.debug.maxToStringFields","1000").
      //enableHiveSupport().
      getOrCreate()
    val sc = spark.sparkContext


    import org.apache.spark.ml.feature.MinMaxScaler

    val dataFrame = spark.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")

    val scaler = new MinMaxScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")

    // Compute summary statistics and generate MinMaxScalerModel
    val scalerModel = scaler.fit(dataFrame)

    // rescale each feature to range [min, max].
    val scaledData = scalerModel.transform(dataFrame)
    scaledData.show()
    sc.stop()
    
  }


}
