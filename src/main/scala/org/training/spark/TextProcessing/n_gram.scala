package org.training.spark.TextProcessing

// 参考自spark官网教程 http://spark.apache.org/docs/latest/ml-features.html#tf-idf
// In the following code segment, we start with a set of sentences.
// We split each sentence into words using Tokenizer. For each sentence (bag of words),
// we use HashingTF to hash the sentence into a feature vector. We use IDF to rescale
// the feature vectors; this generally improves performance when using text as features. // Our feature vectors could then be passed to a learning algorithm.
/**
 *
一个 n-gram是一个长度为n（整数）的字的序列。NGram可以用来将输入特征转换成n-grams。
NGram 的输入为一系列的字符串（例如：Tokenizer分词器的输出）。参数n表示每个n-gram中单词（terms）的数量。
NGram的输出结果是多个n-grams构成的序列，其中，每个n-gram表示被空格分割出来的n个连续的单词。如果输入的字符串少于n个单词，NGram输出为空。
  */


import org.apache.spark.sql.SparkSession

// 创建实例数据

object n_gram {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.
      builder().
      master("local[*]").
      appName("tf-idf").
      config("spark.debug.maxToStringFields","1000").
      //enableHiveSupport().
      getOrCreate()
    val sc = spark.sparkContext

    import org.apache.spark.ml.feature.NGram

    val wordDataFrame = spark.createDataFrame(Seq(
      (0, Array("Hi", "I", "heard", "about", "Spark")),
      (1, Array("I", "wish", "Java", "could", "use", "case", "classes")),
      (2, Array("Logistic", "regression", "models", "are", "neat"))
    )).toDF("label", "words")

    val ngram = new NGram().setInputCol("words").setOutputCol("ngrams").setN(2)
    val ngramDataFrame = ngram.transform(wordDataFrame)
    ngramDataFrame.take(3).map(_.getAs[Stream[String]]("ngrams").toList).foreach(println)
    sc.stop()
    
  }


}
