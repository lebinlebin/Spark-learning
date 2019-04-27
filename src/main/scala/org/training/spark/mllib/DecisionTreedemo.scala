package org.training.spark.mllib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.{SparkConf, SparkContext}
/*
Spark-MLlib实例——决策树
决策树分类的思想类似于找对象。现想象一个女孩的母亲要给这个女孩介绍男朋友，于是有了下面的对话：
女儿：多大年纪了？
母亲：26。
女儿：长的帅不帅？
母亲：挺帅的。
女儿：收入高不？
母亲：不算很高，中等情况。
女儿：是公务员不？
母亲：是，在税务局上班呢。
女儿：那好，我去见见。
 */


  /**
    * 决策树分类
    */
  object DecisionTreedemo {

    def main(args: Array[String]) {

      val conf = new SparkConf().setAppName("DecisionTree").setMaster("local")
      val sc = new SparkContext(conf)
      Logger.getRootLogger.setLevel(Level.WARN)

      //训练数据
      val data1 = sc.textFile("data/DecisionTreedemodataTree1.txt")

      //测试数据
      val data2 = sc.textFile("data/DecisionTreedemodataTree2.txt")


      //转换成向量
      val tree1 = data1.map { line =>
        val parts = line.split(',')
        LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
      }

      val tree2 = data2.map { line =>
        val parts = line.split(',')
        LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
      }

      //赋值
      val (trainingData, testData) = (tree1, tree2)

      //分类
      val numClasses = 2
      val categoricalFeaturesInfo = Map[Int, Int]()
      val impurity = "gini"

      //最大深度
      val maxDepth = 5
      //最大分支
      val maxBins = 32

      //模型训练
      val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
        impurity, maxDepth, maxBins)

      //模型预测
      val labelAndPreds = testData.map { point =>
        val prediction = model.predict(point.features)
        (point.label, prediction)
      }

      //测试值与真实值对比
      val print_predict = labelAndPreds.take(15)
      println("label" + "\t" + "prediction")
      for (i <- 0 to print_predict.length - 1) {
        println(print_predict(i)._1 + "\t" + print_predict(i)._2)
      }

      //树的错误率
      val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
      println("Test Error = " + testErr)
      //打印树的判断值
      println("Learned classification tree model:\n" + model.toDebugString)

//      Learned classification tree model:
//          DecisionTreeModel classifier of depth 4 with 11 nodes
//              If (feature 1 <= 0.0)
//      Predict: 0.0
//      Else (feature 1 > 0.0)
//      If (feature 3 <= 0.0)
//      If (feature 0 <= 30.0)
//      If (feature 2 <= 1.0)
//      Predict: 1.0
//      Else (feature 2 > 1.0)
//      Predict: 0.0
//      Else (feature 0 > 30.0)
//      Predict: 0.0
//      Else (feature 3 > 0.0)
//      If (feature 2 <= 0.0)
//      Predict: 0.0
//      Else (feature 2 > 0.0)
//      Predict: 1.0


    }

  }
