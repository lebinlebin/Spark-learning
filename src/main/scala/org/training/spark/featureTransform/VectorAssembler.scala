package org.training.spark.featureTransform

// 参考自spark官网教程 http://spark.apache.org/docs/latest/ml-features.html#tf-idf
// In the following code segment, we start with a set of sentences.
// We split each sentence into words using Tokenizer. For each sentence (bag of words),
// we use HashingTF to hash the sentence into a feature vector. We use IDF to rescale
// the feature vectors; this generally improves performance when using text as features. // Our feature vectors could then be passed to a learning algorithm.
/**
2.19 VectorAssembler（特征向量合并）
VectorAssembler是一个转换器，它将给定的若干列合并为单列向量。它可以将原始特征和一系列通过其他转换器变换得到的特征合并为单一的特征向量，来训练如逻辑回归和决策树等机器学习算法。VectorAssembler可接受的输入列类型：数值型、布尔型、向量型。输入列的值将按指定顺序依次添加到一个新向量中。
Examples
假设我们有如下DataFrame包含id, hour, mobile, userFeatures以及clicked列：


id | hour | mobile | userFeatures     | clicked
----|------|--------|------------------|---------
 0  | 18   | 1.0    | [0.0, 10.0, 0.5] | 1.0

userFeatures列中含有3个用户特征，我们将hour, mobile以及userFeatures合并为一个新的单一特征向量。将VectorAssembler的输入指定为hour, mobile以及userFeatures，输出指定为features，通过转换我们将得到以下结果：


id | hour | mobile | userFeatures     | clicked | features
----|------|--------|------------------|---------|-----------------------------
 0  | 18   | 1.0    | [0.0, 10.0, 0.5] | 1.0     | [18.0, 1.0, 0.0, 10.0, 0.5]


id | hour | mobile | userFeatures     | clicked | features
----|------|--------|------------------|---------|-----------------------------
 0  | 18   | 1.0    | [0.0, 10.0, 0.5] | 1.0     | [18.0, 1.0, 0.0, 10.0, 0.5]
在Spark repo中路径”examples/src/main/scala/org/apache/spark/examples/ml/VectorAssemblerExample.scala”里可以找到完整的示例代码
  */


import org.apache.spark.sql.SparkSession

// 创建实例数据

object VectorAssembler {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.
      builder().
      master("local[*]").
      appName("tf-idf").
      config("spark.debug.maxToStringFields","1000").
      //enableHiveSupport().
      getOrCreate()
    val sc = spark.sparkContext

    import org.apache.spark.ml.feature.PCA
    import org.apache.spark.ml.linalg.Vectors

    val data = Array(
      Vectors.sparse(5, Seq((1, 1.0), (3, 7.0))),
      Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0),
      Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0)
    )
    val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")
    val pca = new PCA()
      .setInputCol("features")
      .setOutputCol("pcaFeatures")
      .setK(3)
      .fit(df)
    val pcaDF = pca.transform(df)
    val result = pcaDF.select("pcaFeatures")
    result.show()

    sc.stop()
    
  }


}
