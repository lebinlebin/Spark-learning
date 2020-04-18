package org.training.spark.featureTransform

// 参考自spark官网教程 http://spark.apache.org/docs/latest/ml-features.html#tf-idf
// In the following code segment, we start with a set of sentences.
// We split each sentence into words using Tokenizer. For each sentence (bag of words),
// we use HashingTF to hash the sentence into a feature vector. We use IDF to rescale
// the feature vectors; this generally improves performance when using text as features. // Our feature vectors could then be passed to a learning algorithm.
/**
The Discrete Cosine Transform transforms a length N N real-valued sequence in the time domain into another length N N real-valued sequence in the frequency domain.
A DCT class provides this functionality, implementing the DCT-II and scaling the result by 1√ 2 such that the representing matrix for the transform is unitary.
No shift is applied to the transformed sequence (e.g. the 0 0th element of the transformed sequence is the 0 0th DCT coefficient and not the N /2 N/2th).
离散余弦变换（Discrete Cosine Transform） 是将时域的N维实数序列转换成频域的N维实数序列的过程（有点类似离散傅里叶变换）。（ML中的）DCT类提供了离散余弦变换DCT-II的功能，
将离散余弦变换后结果乘以1√2 得到一个与时域矩阵长度一致的矩阵。输入序列与输出之间是一一对应的。
  */


import org.apache.spark.sql.SparkSession

// 创建实例数据

object DiscreteCosineTransform {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.
      builder().
      master("local[*]").
      appName("tf-idf").
      config("spark.debug.maxToStringFields","1000").
      //enableHiveSupport().
      getOrCreate()
    val sc = spark.sparkContext

    import org.apache.spark.ml.feature.DCT
    import org.apache.spark.ml.linalg.Vectors

    val data = Seq(
      Vectors.dense(0.0, 1.0, -2.0, 3.0),
      Vectors.dense(-1.0, 2.0, 4.0, -7.0),
      Vectors.dense(14.0, -2.0, -5.0, 1.0))

    val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")

    val dct = new DCT()
      .setInputCol("features")
      .setOutputCol("featuresDCT")
      .setInverse(false)

    val dctDf = dct.transform(df)
    dctDf.select("featuresDCT").show(3)

    sc.stop()
    
  }


}
