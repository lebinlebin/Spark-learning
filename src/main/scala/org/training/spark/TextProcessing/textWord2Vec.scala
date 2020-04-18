package org.training.spark.TextProcessing

/**
  * Word2Vec是一个通过词向量来表示文档语义上相似度的Estimator(模型评估器)，它会训练出Word2VecModel模型。该模型将（文本的）
  * 每个单词映射到一个单独的大小固定的词向量（该文本对应的）上。Word2VecModel通过文本单词的平均数（条件概率）将每个文档转换为词向量;
  * 此向量可以用作特征预测、 文档相似度计算等。请阅读英文原文Word2Vec MLlib 用户指南了解更多的细节。
  * 在下面的代码段中，我们以一组文档为例，每一组都由一系列的词（序列）构成。通过Word2Vec把每个文档变成一个特征词向量。
  * 这个特征矢量就可以（当做输入参数）传递给机器学习算法。
  */

import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.SparkSession

// 创建实例数据

object textWord2Vec {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.
      builder().
      master("local[*]").
      appName("tf-idf").
      config("spark.debug.maxToStringFields","1000").
      enableHiveSupport().
      getOrCreate()
    val sc = spark.sparkContext


    // Input data: Each row is a bag of words from a sentence or document.
    val documentDF = spark.createDataFrame(Seq(
      "Hi I heard about Spark".split(" "),
      "I wish Java could use case classes".split(" "),
      "Logistic regression models are neat".split(" ")
    ).map(Tuple1.apply)).toDF("text")

    // Learn a mapping from words to Vectors.
    val word2Vec = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(3)
      .setMinCount(0)
    val model = word2Vec.fit(documentDF)
    val result = model.transform(documentDF)
    result.select("result").take(3).foreach(println)
    sc.stop()
    
  }


}
