package org.training.spark.featureTransform.FeatureSelectors

// 参考自spark官网教程 http://spark.apache.org/docs/latest/ml-features.html#tf-idf
// In the following code segment, we start with a set of sentences.
// We split each sentence into words using Tokenizer. For each sentence (bag of words),
// we use HashingTF to hash the sentence into a feature vector. We use IDF to rescale
// the feature vectors; this generally improves performance when using text as features. // Our feature vectors could then be passed to a learning algorithm.
/**
3.1 VectorSlicer（向量选择）
VectorSlicer是一个将输入特征向量转换维输出原始特征向量子集的转换器。VectorSlicer对特征提取非常有帮助。
VectorSlicer接收带有特定索引的向量列，通过对这些索引的值进行筛选得到新的向量集。可接受如下两种索引
整数索引，setIndices()。
字符串索引，setNames()，此类要求向量列有AttributeGroup，因为该工具根据Attribute来匹配属性字段。

可以指定整数或者字符串类型。另外，也可以同时使用整数索引和字符串名字。不允许使用重复的特征，所以所选的索引或者名字必须是独一的。注意如果使用名字特征，当遇到空值的时候将会抛异常。
输出将会首先按照所选的数字索引排序（按输入顺序），其次按名字排序（按输入顺序）。
Examples
假设我们有一个DataFrame含有userFeatures列：

userFeatures
------------------
 [0.0, 10.0, 0.5]

userFeatures是一个包含3个用户特征的特征向量。假设userFeatures的第一列全为0，我们希望删除它并且只选择后两项。我们可以通过索引setIndices(1, 2)来选择后两项并产生一个新的features列：

userFeatures     | features
------------------|-----------------------------
 [0.0, 10.0, 0.5] | [10.0, 0.5]

假设我们还有如同[“f1”, “f2”, “f3”]的属性，可以通过名字setNames(“f2”, “f3”)的形式来选择：

userFeatures     | features
------------------|-----------------------------
 [0.0, 10.0, 0.5] | [10.0, 0.5]
 ["f1", "f2", "f3"] | ["f2", "f3"]
  */


import org.apache.spark.sql.SparkSession

// 创建实例数据

object VectorSlicer {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.
      builder().
      master("local[*]").
      appName("tf-idf").
      config("spark.debug.maxToStringFields","1000").
      //enableHiveSupport().
      getOrCreate()
    val sc = spark.sparkContext

    import java.util.Arrays

    import org.apache.spark.ml.attribute.{Attribute, AttributeGroup, NumericAttribute}
    import org.apache.spark.ml.feature.VectorSlicer
    import org.apache.spark.ml.linalg.Vectors
    import org.apache.spark.sql.Row
    import org.apache.spark.sql.types.StructType

    val data = Arrays.asList(Row(Vectors.dense(-2.0, 2.3, 0.0)))

    val defaultAttr = NumericAttribute.defaultAttr
    val attrs = Array("f1", "f2", "f3").map(defaultAttr.withName)
    val attrGroup = new AttributeGroup("userFeatures", attrs.asInstanceOf[Array[Attribute]])

    val dataset = spark.createDataFrame(data, StructType(Array(attrGroup.toStructField())))

    val slicer = new VectorSlicer().setInputCol("userFeatures").setOutputCol("features")

    slicer.setIndices(Array(1)).setNames(Array("f3"))
    // or slicer.setIndices(Array(1, 2)), or slicer.setNames(Array("f2", "f3"))

    val output = slicer.transform(dataset)
    println(output.select("userFeatures", "features").first())

    sc.stop()
    
  }


}
