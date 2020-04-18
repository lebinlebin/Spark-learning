package org.training.spark.featureTransform

// 参考自spark官网教程 http://spark.apache.org/docs/latest/ml-features.html#tf-idf
// In the following code segment, we start with a set of sentences.
// We split each sentence into words using Tokenizer. For each sentence (bag of words),
// we use HashingTF to hash the sentence into a feature vector. We use IDF to rescale
// the feature vectors; this generally improves performance when using text as features. // Our feature vectors could then be passed to a learning algorithm.
/**
StandardScaler（z-score规范化：零均值标准化）可以将输入的一组Vector特征向量规范化（标准化），使其有统一的的标准差（均方差？）以及均值为0。它需要如下参数：
1. withStd：默认值为真，将数据缩放到统一标准差方式。
2. withMean：默认为假。将均值为0。该方法将产出一个稠密的输出向量，所以不适用于稀疏向量。
StandardScaler是一个Estimator，它可以通过拟合（fit）数据集产生一个StandardScalerModel，用来统计汇总。StandardScalerModel可以用来将向量转换至统一的标准差以及（或者）零均值特征。
注意如果特征的标准差为零，则该特征在向量中返回的默认值为0.0。
下面的示例展示如果读入一个libsvm形式的数据以及返回有统一标准差的标准化特征。
  */


import org.apache.spark.sql.SparkSession

// 创建实例数据

object StandardScaler {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.
      builder().
      master("local[*]").
      appName("tf-idf").
      config("spark.debug.maxToStringFields","1000").
      //enableHiveSupport().
      getOrCreate()
    val sc = spark.sparkContext

    import org.apache.spark.ml.feature.StandardScaler

    val dataFrame = spark.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")

    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
      .setWithStd(true)
      .setWithMean(false)

    // Compute summary statistics by fitting the StandardScaler.
    val scalerModel = scaler.fit(dataFrame)

    // Normalize each feature to have unit standard deviation.
    val scaledData = scalerModel.transform(dataFrame)
    scaledData.show()
    sc.stop()
    
  }


}
