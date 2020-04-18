package org.training.spark.featureTransform.FeatureSelectors

// 参考自spark官网教程 http://spark.apache.org/docs/latest/ml-features.html#tf-idf
// In the following code segment, we start with a set of sentences.
// We split each sentence into words using Tokenizer. For each sentence (bag of words),
// we use HashingTF to hash the sentence into a feature vector. We use IDF to rescale
// the feature vectors; this generally improves performance when using text as features. // Our feature vectors could then be passed to a learning algorithm.
/**
3.2 RFormula（R模型公式）
RFormula通过R模型公式（R model formula）来将数据中的字段转换成特征值。ML只支持R操作中的部分操作，包括‘~’, ‘.’, ‘:’, ‘+’以及‘-‘，基本操作如下：
~分隔目标和对象
+合并对象，“+ 0”意味着删除空格
:交互（数值相乘，类别二元化）
. 除了目标外的全部列
假设有双精度的a和b两列，RFormula的使用用例如下
y ~ a + b表示模型y ~ w0 + w1 * a +w2 * b其中w0为截距，w1和w2为相关系数。
y ~a + b + a:b – 1表示模型y ~ w1* a + w2 * b + w3 * a * b，其中w1，w2，w3是相关系数。
RFormula产生一个特征向量和一个double或者字符串标签列（label）。就如R中使用formulas一样，字符型的输入将转换成one-hot编码，数字输入转换成双精度。如果类别列是字符串类型，它将通过StringIndexer转换为double类型索引。如果标签列不存在，则formulas输出中将通过特定的响应变量创造一个标签列。
Examples
假设我们有一个DataFrame含有id,country, hour和clicked四列：


id | country | hour | clicked
---|---------|------|---------
 7 | "US"    | 18   | 1.0
 8 | "CA"    | 12   | 0.0
 9 | "NZ"    | 15   | 0.0

如果使用RFormula公式clicked ~ country + hour，则表明我们希望基于country 和hour预测clicked，通过转换我们可以得到如下DataFrame：


id | country | hour | clicked | features         | label
---|---------|------|---------|------------------|-------
 7 | "US"    | 18   | 1.0     | [0.0, 0.0, 18.0] | 1.0
 8 | "CA"    | 12   | 0.0     | [0.0, 1.0, 12.0] | 0.0
 9 | "NZ"    | 15   | 0.0     | [1.0, 0.0, 15.0] | 0.0
  */


import org.apache.spark.sql.SparkSession

// 创建实例数据

object RFormula {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.
      builder().
      master("local[*]").
      appName("tf-idf").
      config("spark.debug.maxToStringFields","1000").
      //enableHiveSupport().
      getOrCreate()
    val sc = spark.sparkContext

    import org.apache.spark.ml.feature.RFormula

    val dataset = spark.createDataFrame(Seq(
      (7, "US", 18, 1.0),
      (8, "CA", 12, 0.0),
      (9, "NZ", 15, 0.0)
    )).toDF("id", "country", "hour", "clicked")
    val formula = new RFormula()
      .setFormula("clicked ~ country + hour")
      .setFeaturesCol("features")
      .setLabelCol("label")
    val output = formula.fit(dataset).transform(dataset)
    output.select("features", "label").show()

    sc.stop()
    
  }


}
