package org.training.spark.featureTransform

// 参考自spark官网教程 http://spark.apache.org/docs/latest/ml-features.html#tf-idf
// In the following code segment, we start with a set of sentences.
// We split each sentence into words using Tokenizer. For each sentence (bag of words),
// we use HashingTF to hash the sentence into a feature vector. We use IDF to rescale
// the feature vectors; this generally improves performance when using text as features. // Our feature vectors could then be passed to a learning algorithm.
/**
StringIndexer（字符串-索引变换）将字符串的（以单词为）标签编码成标签索引（表示）。标签索引序列的取值范围是[0，numLabels（字符串中所有出现的单词去掉重复的词后的总和）]，按照标签出现频率排序，出现最多的标签索引为0。如果输入是数值型，我们先将数值映射到字符串，再对字符串进行索引化。如果下游的pipeline（例如：Estimator或者Transformer）需要用到索引化后的标签序列，则需要将这个pipeline的输入列名字指定为索引化序列的名字。大部分情况下，通过setInputCol设置输入的列名。
Examples
假设我们有如下的DataFrame，包含有id和category两列

id | category
----|----------
 0  | a
 1  | b
 2  | c
 3  | a
 4  | a
 5  | c

标签类别（category）是有3种取值的标签：“a”，“b”，“c”。使用StringIndexer通过category进行转换成categoryIndex后可以得到如下结果：


id | category | categoryIndex
----|----------|---------------
 0  | a        | 0.0
 1  | b        | 2.0
 2  | c        | 1.0
 3  | a        | 0.0
 4  | a        | 0.0
 5  | c        | 1.0
“a”因为出现的次数最多，所以得到为0的索引（index）。“c”得到1的索引，“b”得到2的索引
另外，StringIndexer在转换新数据时提供两种容错机制处理训练中没有出现的标签
StringIndexer抛出异常错误（默认值）
跳过未出现的标签实例。
Examples
回顾一下上一个例子，这次我们将继续使用上一个例子训练出来的StringIndexer处理下面的数据集

id | category
----|----------
 0  | a
 1  | b
 2  | c
 3  | d
如果没有在StringIndexer里面设置未训练过（unseen）的标签的处理或者设置未“error”，运行时会遇到程序抛出异常。当然，也可以通过设置setHandleInvalid(“skip”)，得到如下的结果


id | category | categoryIndex
----|----------|---------------
 0  | a        | 0.0
 1  | b        | 2.0
 2  | c        | 1.0

注意：输出里面没有出现“d”
  */


import org.apache.spark.sql.SparkSession

// 创建实例数据

object StringIndexer {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.
      builder().
      master("local[*]").
      appName("tf-idf").
      config("spark.debug.maxToStringFields","1000").
      //enableHiveSupport().
      getOrCreate()
    val sc = spark.sparkContext

    import org.apache.spark.ml.feature.StringIndexer

    val df = spark.createDataFrame(
      Seq((0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c"))
    ).toDF("id", "category")

    val indexer = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("categoryIndex")

    val indexed = indexer.fit(df).transform(df)
    indexed.show()

    sc.stop()
    
  }


}
