package org.training.spark.TextProcessing

// 参考自spark官网教程 http://spark.apache.org/docs/latest/ml-features.html#tf-idf
// In the following code segment, we start with a set of sentences.
// We split each sentence into words using Tokenizer. For each sentence (bag of words),
// we use HashingTF to hash the sentence into a feature vector. We use IDF to rescale
// the feature vectors; this generally improves performance when using text as features. // Our feature vectors could then be passed to a learning algorithm.
/**
 *
 * Tokenization（文本符号化）是将文本 （如一个句子）拆分成单词的过程。（在Spark ML中）Tokenizer（分词器）提供此功能。下面的示例演示如何将句子拆分为词的序列。
 * RegexTokenizer提供了（更高级的）基于正则表达式 (regex) 匹配的（对句子或文本的）单词拆分。
 * 默认情况下，参数”pattern”(默认的正则表达式: “\s+”) 作为分隔符用于拆分输入的文本。或者，用户可以将参数“gaps”设置为 false ，
 * 指定正则表达式”pattern”表示”tokens”，而不是分隔符，这样作为分词结果找到的所有匹配项。
  */


import org.apache.spark.sql.SparkSession

// 创建实例数据

object Tokenizer {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.
      builder().
      master("local[*]").
      appName("tf-idf").
      config("spark.debug.maxToStringFields","1000").
      //enableHiveSupport().
      getOrCreate()
    val sc = spark.sparkContext

    import org.apache.spark.ml.feature.{RegexTokenizer, Tokenizer}

    val sentenceDataFrame = spark.createDataFrame(Seq(
      (0, "Hi I heard about Spark"),
      (1, "I wish Java could use case classes"),
      (2, "Logistic,regression,models,are,neat")
    )).toDF("label", "sentence")

    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val regexTokenizer = new RegexTokenizer()
      .setInputCol("sentence")
      .setOutputCol("words")
      .setPattern("\\W") // alternatively .setPattern("\\w+").setGaps(false)

    val tokenized = tokenizer.transform(sentenceDataFrame)
    tokenized.select("words", "label").take(3).foreach(println)
    val regexTokenized = regexTokenizer.transform(sentenceDataFrame)
    regexTokenized.select("words", "label").take(3).foreach(println)


    sc.stop()
    
  }


}
