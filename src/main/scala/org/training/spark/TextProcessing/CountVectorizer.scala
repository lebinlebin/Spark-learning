package org.training.spark.TextProcessing

// 参考自spark官网教程 http://spark.apache.org/docs/latest/ml-features.html#tf-idf
// In the following code segment, we start with a set of sentences.
// We split each sentence into words using Tokenizer. For each sentence (bag of words),
// we use HashingTF to hash the sentence into a feature vector. We use IDF to rescale
// the feature vectors; this generally improves performance when using text as features. // Our feature vectors could then be passed to a learning algorithm.
/**
  * CountVectorizer和CountVectorizerModel旨在通过计数将文本文档转换为特征向量。
  * 当不存在先验字典时，CountVectorizer可以作为Estimator提取词汇，并生成CountVectorizerModel。该模型产生关于该文档词汇的稀疏表示（稀疏特征向量），
  * 这个表示（特征向量）可以传递给其他像 LDA 算法。
  * 在拟合fitting过程中， CountVectorizer将根据语料库中的词频排序选出前vocabSize个词。
  * 其中一个配置参数minDF通过指定词汇表中的词语在文档中出现的最小次数 (或词频 if < 1.0) ，影响拟合（fitting）的过程。
  * 另一个可配置的二进制toggle参数控制输出向量。如果设置为 true 那么所有非零计数设置为 1。这对于二元型离散概率模型非常有用。
  */
/**
  * Examples
  * 假设我们有如下的DataFrame包含id和texts两列：
  *
  * id | texts
  * ----|----------
  * 0  | Array("a", "b", "c")
  * 1  | Array("a", "b", "b", "c", "a")
  *
  * 文本中每行都是一个文本类型的数组（字符串）。调用CountVectorizer产生词汇表（a, b, c）的CountVectorizerModel模型，转后后的输出向量如下：
  *
  * id | texts                           | vector
  * ----|---------------------------------|---------------
  * 0  | Array("a", "b", "c")            | (3,[0,1,2],[1.0,1.0,1.0])
  * 1  | Array("a", "b", "b", "c", "a")  | (3,[0,1,2],[2.0,2.0,1.0])
  */

import org.apache.spark.sql.SparkSession

// 创建实例数据

object CountVectorizer {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.
      builder().
      master("local[*]").
      appName("tf-idf").
      config("spark.debug.maxToStringFields","1000").
      //enableHiveSupport().
      getOrCreate()
    val sc = spark.sparkContext

    /*
    每个向量表示文档词汇表中每个词语出现的次数
     */

    import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}

    val df = spark.createDataFrame(Seq(
      (0, Array("a", "b", "c")),
      (1, Array("a", "b", "b", "c", "a"))
    )).toDF("id", "words")

    // fit a CountVectorizerModel from the corpus
    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("features")
      .setVocabSize(3)
      .setMinDF(2)
      .fit(df)

    // alternatively, define CountVectorizerModel with a-priori vocabulary
    val cvm = new CountVectorizerModel(Array("a", "b", "c"))
      .setInputCol("words")
      .setOutputCol("features")

    cvModel.transform(df).select("features").show()


    sc.stop()
    
  }


}
