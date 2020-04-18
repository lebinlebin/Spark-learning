package org.training.spark.featureTransform

// 参考自spark官网教程 http://spark.apache.org/docs/latest/ml-features.html#tf-idf
// In the following code segment, we start with a set of sentences.
// We split each sentence into words using Tokenizer. For each sentence (bag of words),
// we use HashingTF to hash the sentence into a feature vector. We use IDF to rescale
// the feature vectors; this generally improves performance when using text as features. // Our feature vectors could then be passed to a learning algorithm.
/**
2.11 VectorIndexer(向量类型索引化)
VectorIndexer是对数据集特征向量中的类别特征（index categorical features categorical features ，eg：枚举类型）进行编号索引。它能够自动判断那些特征是可以重新编号的类别型，并对他们进行重新编号索引，具体做法如下：
1.获得一个向量类型的输入以及maxCategories参数。
2.基于原始向量数值识别哪些特征需要被类别化：特征向量中某一个特征不重复取值个数小于等于maxCategories则认为是可以重新编号索引的。某一个特征不重复取值个数大于maxCategories，则该特征视为连续值，不会重新编号（不会发生任何改变）
3.对于每一个可编号索引的类别特征重新编号为0～K（K<=maxCategories-1）。
4.对类别特征原始值用编号后的索引替换掉。
索引后的类别特征可以帮助决策树等算法处理类别型特征，提高性能。
在下面的例子中，我们读入一个数据集，然后使用VectorIndexer来决定哪些类别特征需要被作为索引类型处理，将类型特征转换为他们的索引。转换后的数据可以传递给DecisionTreeRegressor之类的算法出来类型特征。
简单理解一下：以C为例，假如一个星期的枚举型的类型enum weekday{ sun = 4,mou =5, tue =6, wed = 7, thu =8, fri = 9, sat =10 };如果需要进行这个特征带入运算，可以将这些枚举数值重新编号为
{ sun = 0 , mou =1, tue =2, wed = 3, thu =4, fri = 5, sat =6 }，通常是出现次数越多的枚举，编号越小（从0开始）
  */


import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

// 创建实例数据

object VectorIndexer {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.
      builder().
      master("local[*]").
      appName("tf-idf").
      config("spark.debug.maxToStringFields","1000").
      //enableHiveSupport().
      getOrCreate()
    val sc = spark.sparkContext

    import org.apache.spark.ml.feature.VectorIndexer

    //val data = spark.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")
    val data1 = Seq(
      Vectors.dense(2, 5, 7, 3),
      Vectors.dense(4, 2, 4, 7),
      Vectors.dense(5, 3, 4, 7),
      Vectors.dense(6, 2, 4, 7),
      Vectors.dense(7, 2, 4, 7),
      Vectors.dense(8, 2, 5, 1))

    val data = spark.createDataFrame(data1.map(Tuple1.apply)).toDF("features")

    val indexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexed")
      .setMaxCategories(10)

    val indexerModel = indexer.fit(data)

    val categoricalFeatures: Set[Int] = indexerModel.categoryMaps.keys.toSet
    println(s"Chose ${categoricalFeatures.size} categorical features: " +
      categoricalFeatures.mkString(", "))

    // Create new column "indexed" with categorical values transformed to indices
    val indexedData = indexerModel.transform(data)
    indexedData.show()
    sc.stop()
    
  }


}
