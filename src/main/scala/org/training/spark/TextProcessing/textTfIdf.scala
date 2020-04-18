package org.training.spark.TextProcessing

// 参考自spark官网教程 http://spark.apache.org/docs/latest/ml-features.html#tf-idf
// In the following code segment, we start with a set of sentences.
// We split each sentence into words using Tokenizer. For each sentence (bag of words),
// we use HashingTF to hash the sentence into a feature vector. We use IDF to rescale
// the feature vectors; this generally improves performance when using text as features. // Our feature vectors could then be passed to a learning algorithm.

import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.sql.SparkSession

// 创建实例数据

object textTfIdf {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.
      builder().
      master("local[*]").
      appName("tf-idf").
      config("spark.debug.maxToStringFields","1000").
//      enableHiveSupport().
      getOrCreate()
    val sc = spark.sparkContext


    val sentenceData = spark.createDataFrame(Seq(
      (0, "Hi I heard about Spark"),
      (0, "I wish Java could use case classes"),
      (1, "Logistic regression models are neat")
    )).toDF("label", "sentence")

    sentenceData.show
    //  +-----+--------------------+
    //  |label|            sentence|
    //  +-----+--------------------+
    //  |    0|Hi I heard about ...|
    //  |    0|I wish Java could...|
    //  |    1|Logistic regressi...|
    //  +-----+--------------------+

    //句子转化成单词数组
    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val wordsData = tokenizer.transform(sentenceData)

    // scala> wordsData.show
    //  +-----+--------------------+--------------------+
    //  |label|            sentence|               words|
    //  +-----+--------------------+--------------------+
    //  |    0|Hi I heard about ...|ArrayBuffer(hi, i...|
    //  |    0|I wish Java could...|ArrayBuffer(i, wi...|
    //  |    1|Logistic regressi...|ArrayBuffer(logis...|
    //  +-----+--------------------+--------------------+

    // hashing计算TF值,同时还把停用词(stop words)过滤掉了. setNumFeatures(20)表最多20个词
    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)
    val featurizedData = hashingTF.transform(wordsData)

    // scala> featurizedData.show
    //  +-----+--------------------+--------------------+--------------------+
    //  |label|            sentence|               words|         rawFeatures|
    //  +-----+--------------------+--------------------+--------------------+
    //  |    0|Hi I heard about ...|ArrayBuffer(hi, i...|(20,[5,6,9],[2.0,...|
    //  |    0|I wish Java could...|ArrayBuffer(i, wi...|(20,[3,5,12,14,18...|
    //  |    1|Logistic regressi...|ArrayBuffer(logis...|(20,[5,12,14,18],...|
    //  +-----+--------------------+--------------------+--------------------+


    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)
    val rescaledData = idfModel.transform(featurizedData)
    // 提取该数据中稀疏向量的数据,稀疏向量:SparseVector(size,indices,values)
    // rescaledData.select("features").rdd.map(row => row.getAs[linalg.Vector](0)).map(x => x.toSparse.indices).collect
    rescaledData.select("features", "label").take(3).foreach(println)

    //  [(20,[5,6,9],[0.0,0.6931471805599453,1.3862943611198906]),0]
    //  [(20,[3,5,12,14,18],[1.3862943611198906,0.0,0.28768207245178085,0.28768207245178085,0.28768207245178085]),0]
    //  [(20,[5,12,14,18],[0.0,0.5753641449035617,0.28768207245178085,0.28768207245178085]),1]
    // 其中,20是标签总数,下一项是单词对应的hashing ID.最后是TF-IDF结果



    sc.stop()
    
  }


}
