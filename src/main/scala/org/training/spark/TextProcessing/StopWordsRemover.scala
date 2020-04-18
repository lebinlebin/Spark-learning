package org.training.spark.TextProcessing

// 参考自spark官网教程 http://spark.apache.org/docs/latest/ml-features.html#tf-idf
// In the following code segment, we start with a set of sentences.
// We split each sentence into words using Tokenizer. For each sentence (bag of words),
// we use HashingTF to hash the sentence into a feature vector. We use IDF to rescale
// the feature vectors; this generally improves performance when using text as features. // Our feature vectors could then be passed to a learning algorithm.
/**
 * Stop words （停用字）是（在文档中）频繁出现，但未携带太多意义的词语，它们不应该参与算法运算。
 * StopWordsRemover（的作用是）将输入的字符串 （如分词器Tokenizer的输出）中的停用字删除（后输出）。停用字表由stopWords参数指定。对于某些语言的默认停止词是通过调用StopWordsRemover.loadDefaultStopWords(language)设置的，可用的选项为”丹麦”，”荷兰语”、”英语”、”芬兰语”，”法国”，”德国”、”匈牙利”、”意大利”、”挪威”、”葡萄牙”、”俄罗斯”、”西班牙”、”瑞典”和”土耳其”。布尔型参数caseSensitive指示是否区分大小写 （默认为否）。
 *
 * Examples
 * 假设有如下DataFrame，有id和raw两列：
 *
 * id | raw
 * ----|----------
 * 0  | [I, saw, the, red, baloon]
 * 1  | [Mary, had, a, little, lamb]
 * 通过对raw列调用StopWordsRemover，我们可以得到筛选出的结果列如下：
 *
 *
 * id | raw                         | filtered
 * ----|-----------------------------|--------------------
 * 0  | [I, saw, the, red, baloon]  |  [saw, red, baloon]
 * 1  | [Mary, had, a, little, lamb]|[Mary, little, lamb]
 * 其中，“I”, “the”, “had”以及“a”被移除。
 */


import org.apache.spark.sql.SparkSession

// 创建实例数据

object StopWordsRemover {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.
      builder().
      master("local[*]").
      appName("tf-idf").
      config("spark.debug.maxToStringFields","1000").
      //enableHiveSupport().
      getOrCreate()
    val sc = spark.sparkContext

    import org.apache.spark.ml.feature.StopWordsRemover

    val remover = new StopWordsRemover()
      .setInputCol("raw")
      .setOutputCol("filtered")

    val dataSet = spark.createDataFrame(Seq(
      (0, Seq("I", "saw", "the", "red", "baloon")),
      (1, Seq("Mary", "had", "a", "little", "lamb"))
    )).toDF("id", "raw")

    remover.transform(dataSet).show()

    sc.stop()
    
  }


}
