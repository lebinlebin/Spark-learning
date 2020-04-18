package org.training.spark.featureTransform

// 参考自spark官网教程 http://spark.apache.org/docs/latest/ml-features.html#tf-idf
// In the following code segment, we start with a set of sentences.
// We split each sentence into words using Tokenizer. For each sentence (bag of words),
// we use HashingTF to hash the sentence into a feature vector. We use IDF to rescale
// the feature vectors; this generally improves performance when using text as features. // Our feature vectors could then be passed to a learning algorithm.
/**
2.18 SQLTransformer（SQL变换）
SQLTransformer用来转换由SQL定义的陈述。目前仅支持SQL语法如”SELECT … FROM THIS …”，其中”THIS“代表输入数据的基础表。选择语句指定输出中展示的字段、元素和表达式，支持Spark SQL中的所有选择语句。用户可以基于选择结果使用Spark SQL建立方程或者用户自定义函数（UDFs）。SQLTransformer支持语法示例如下：
SELECT a, a + b AS a_b FROM THIS
SELECT a, SQRT(b) AS b_sqrt FROM THIS where a > 5
SELECT a, b, SUM(c) AS c_sum FROM THIS GROUP BY a, b

Examples
假设我们有如下DataFrame包含id，v1，v2列：

id |  v1 |  v2
----|-----|-----
 0  | 1.0 | 3.0
 2  | 2.0 | 5.0

使用SQLTransformer语句”SELECT , (v1 + v2) AS v3, (v1 v2) AS v4 FROM THIS“转换后得到输出如下：
id |  v1 |  v2 |  v3 |  v4
----|-----|-----|-----|-----
 0  | 1.0 | 3.0 | 4.0 | 3.0
 2  | 2.0 | 5.0 | 7.0 |10.0
  */


import org.apache.spark.sql.SparkSession

// 创建实例数据

object SQLTransformer {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.
      builder().
      master("local[*]").
      appName("tf-idf").
      config("spark.debug.maxToStringFields","1000").
      //enableHiveSupport().
      getOrCreate()
    val sc = spark.sparkContext

    import org.apache.spark.ml.feature.SQLTransformer

    val df = spark.createDataFrame(
      Seq((0, 1.0, 3.0), (2, 2.0, 5.0))).toDF("id", "v1", "v2")

    val sqlTrans = new SQLTransformer().setStatement(
      "SELECT *, (v1 + v2) AS v3, (v1 * v2) AS v4 FROM __THIS__")

    sqlTrans.transform(df).show()

    sc.stop()
    
  }


}
