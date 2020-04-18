package org.training.spark.featureTransform

// 参考自spark官网教程 http://spark.apache.org/docs/latest/ml-features.html#tf-idf
// In the following code segment, we start with a set of sentences.
// We split each sentence into words using Tokenizer. For each sentence (bag of words),
// we use HashingTF to hash the sentence into a feature vector. We use IDF to rescale
// the feature vectors; this generally improves performance when using text as features. // Our feature vectors could then be passed to a learning algorithm.
/**
Bucketizer将一列连续的特征转换为（离散的）特征区间，区间由用户指定。参数如下：
splits（分箱数）：分箱数为n+1时，将产生n个区间。除了最后一个区间外，每个区间范围［x,y］由分箱的x，y决定。分箱必须是严格递增的。分箱（区间）见在分箱（区间）指定外的值将被归为错误。两个分裂的例子为Array(Double.NegativeInfinity, 0.0, 1.0, Double.PositiveInfinity)以及Array(0.0, 1.0, 2.0)。
注意：
当不确定分裂的上下边界时，应当添加Double.NegativeInfinity和Double.PositiveInfinity以免越界。
分箱区间必须严格递增，例如： s0 < s1 < s2 < … < sn
下面将展示Bucketizer的使用方法。
  */


import org.apache.spark.sql.SparkSession

// 创建实例数据

object Bucketizer {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.
      builder().
      master("local[*]").
      appName("tf-idf").
      config("spark.debug.maxToStringFields","1000").
      //enableHiveSupport().
      getOrCreate()
    val sc = spark.sparkContext


    import org.apache.spark.ml.feature.Bucketizer

    val splits = Array(Double.NegativeInfinity, -0.5, 0.0, 0.5, Double.PositiveInfinity)

    val data = Array(-0.5, -0.3, 0.0, 0.2)
    val dataFrame = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")

    val bucketizer = new Bucketizer()
      .setInputCol("features")
      .setOutputCol("bucketedFeatures")
      .setSplits(splits)

    // Transform original data into its bucket index.
    val bucketedData = bucketizer.transform(dataFrame)
    bucketedData.show()
    sc.stop()
    
  }


}
