package org.training.spark.mllib

import org.apache.spark.rdd._
import org.apache.spark.{SparkConf, SparkContext}

/**
 *本代码为Spark机器学习书籍的第五章参考程序
 * 第5章 Spark构建分类模型
 */
object MovieLensClassify5 {

  def main(args: Array[String]) {
    /*
    5.2 从数据中抽取合适的特征
    回顾第3章，可以发现大部分机器学习模型以特征向量的形式处理数值数据。另外，对于分
    类和回归等监督学习方法，需要将目标变量（或者多类别情况下的变量）和特征向量放在一起。
    MLlib中的分类模型通过LabeledPoint对象操作，其中封装了目标变量（ 标签）和特征向量：
        case class LabeledPoint(label: Double, features: Vector)
    虽然在使用分类模型的很多样例中会碰到向量格式的数据集，但在实际工作中，通常还需要
    从原始数据中抽取特征。正如前几章介绍的，这包括封装数值特征、归一或者正则化特征，以及
    使用1-of-k编码表示类属特征
    从Kaggle/StumbleUpon evergreen分类数据集中抽取特征
    考虑到推荐模型中的MovieLens数据集和分类问题无关，本章将使用另外一个数据集。这个
    数据集源自Kaggle比赛，由StumbleUpon提供。比赛的问题涉及网页中推荐的页面是短暂（短暂
    存在，很快就不流行了）还是长久（长时间流行）。
    http://www.kaggle.com/c/stumbleupon/data
     */
    var dataPath = "/Users/liulebin/Documents/codeing/codeingForSelfStudy/Spark_Study/Spark-learning/data/stumbleupon/train_noheader.tsv"
    val conf = new SparkConf().setAppName("MovieLensRecommod")
    if(args.length > 0) {
      dataPath = args(0)
    } else {
      conf.setMaster("local[1]")
    }

    val sc = new SparkContext(conf)

    /*
    开始之前，为了让Spark更好地操作数据，我们需要删除文件第一行的列头名称。进入数据
    的 目 录 （ 这 里 用 PATH 表 示 ） ， 然 后 输 入 如 下 命 令 删 除 第 一 行 并 且 通 过 管 道 保 存 到 以
    train_noheader.tsv命名的新文件中：
    sed 1d train.tsv > train_noheader.tsv
    可以查看上面的数据集页面中的简介得知可用的字段。开始四列分别包含URL、页面的ID、
    原始的文本内容和分配给页面的类别。接下来22列包含各种各样的数值或者类属特征。最后一列
    为目标值， -1为长久， 0为短暂。

    我们将用简单的方法直接对数值特征做处理。因为每个类属变量是二元的，对这些变量已有
    一个用1-of-k编码的特征，于是不需要额外提取特征。
    由于数据格式的问题，我们做一些数据清理的工作，在处理过程中把额外的（"）去掉。数
    据集中还有一些用"?"代替的缺失数据，本例中，我们直接用0替换那些缺失数据：
     */
    // load raw data
    val rawData = sc.textFile(dataPath)
    val records = rawData.map(line => line.split("\t"))
    println(records.first)
    // Array[String] = Array("http://www.bloomberg.com/news/2010-12-23/ibm-predicts-holographic-calls-air-breathing-batteries-by-2015.html", "4042", ...

    /*
    在清理和处理缺失数据后，我们提取最后一列的标记变量以及第5列到第25列的特征矩阵。
    将标签变量转换为Int值，特征向量转换为Double数组。最后，我们将标签和和特征向量转换为
    LabeledPoint实例，从而将特征向量存储到MLlib的Vector中。
     */
    import org.apache.spark.mllib.regression.LabeledPoint
    import org.apache.spark.mllib.linalg.Vectors
    val data = records.map { r: Array[String] =>
      val trimmed = r.map(_.replaceAll("\"", ""))
      val label = trimmed(r.size - 1).toInt//获得label数据
      val features = trimmed.slice(4, r.size - 1).map(d => if (d == "?") 0.0 else d.toDouble)
      LabeledPoint(label, Vectors.dense(features))
    }

    data.cache
    val numData = data.count
    println(numData)
    // numData: Long = 7395
    // 可以看到numData的值为7395。

    /*
    在对数据集做进一步处理之前，我们发现数值数据中包含负的特征值。我们知道，朴素贝叶
    斯模型要求特征值非负，否则碰到负的特征值程序会抛出错误。因此，需要为朴素贝叶斯模型构
    建一份输入特征向量的数据，将负特征值设为0：
    */
    // note that some of our data contains negative feature vaues. For naive Bayes we convert these to zeros
    val nbData = records.map { r =>
      val trimmed = r.map(_.replaceAll("\"", ""))
      val label = trimmed(r.size - 1).toInt
      val features = trimmed.slice(4, r.size - 1).map(d => if (d == "?") 0.0 else d.toDouble).map(d => if (d < 0) 0.0 else d)
      LabeledPoint(label, Vectors.dense(features))
    }


    /**
     * 5.3 训练分类模型
     * 现在我们已经从数据集中提取了基本的特征并且创建了RDD，接下来开始训练各种模型吧。
     * 为了比较不同模型的性能，我们将训练逻辑回归、 SVM、朴素贝叶斯和决策树。你会发现每个模
     * 型的训练方法几乎一样，不同的是每个模型都有着自己特定可配置的模型参数。 MLlib大多数情
     * 况下会设置明确的默认值，但实际上，最好的参数配置需要通过评估技术来选择
     * 在Kaggle/StumbleUpon evergreen的分类数据集中训练分类模型
     * 现在可以对输入数据应用MLlib的模型。首先，需要引入必要的类并对每个模型配置一些基
     * 本的输入参数。其中，需要为逻辑回归和SVM设置迭代次数，为决策树设置最大树深度。
     */
    // train a Logistic Regression model
    import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
    import org.apache.spark.mllib.classification.SVMWithSGD
    import org.apache.spark.mllib.classification.NaiveBayes
    import org.apache.spark.mllib.tree.DecisionTree
    import org.apache.spark.mllib.tree.configuration.Algo
    import org.apache.spark.mllib.tree.impurity.Entropy

    val numIterations = 10
    val maxTreeDepth = 5
    /*
    Symbol LogisticRegressionWithSGD is deprecated. Use ml.classification.LogisticRegression or LogisticRegressionWithLBFGS
     */
    val lrModel = LogisticRegressionWithSGD.train(data, numIterations)
    val svmModel = SVMWithSGD.train(data, numIterations)
    //接下来训练朴素贝叶斯，记住要使用处理过的没有负特征值的数据：
    // note we use nbData here for the NaiveBayes model training
    val nbModel = NaiveBayes.train(nbData)
    //注意，在决策树中，我们设置模式或者Algo时使用了Entropy不纯度估计。
    val dtModel = DecisionTree.train(data, Algo.Classification, Entropy, maxTreeDepth)

    /**
     * 5.4 使用分类模型
     * 现在我们有四个在输入标签和特征下训练好的模型，接下来看看如何使用这些模型进行预
     * 测。目前，我们将使用同样的训练数据来解释每个模型的预测方法。
     * 在Kaggle/StumbleUpon evergreen数据集上进行预测
     * 这里以逻辑回归模型为例（其他模型处理方法类似）：
     */
    // make prediction on a single data point
    val dataPoint = data.first
    // dataPoint: org.apache.spark.mllib.regression.LabeledPoint = LabeledPoint(0.0, [0.789131,2.055555556,0.676470588, ...
    val prediction: Double = lrModel.predict(dataPoint.features)
    /*
    可以看到对于训练数据中第一个样本，模型预测值为1，即长久。让我们来检验一下这个样本真正的标签,可以看到0.0，这个样例中我们的模型预测出错了！
    */
    println(prediction)
    // prediction: Double = 1.0
    val trueLabel = dataPoint.label
    println("trueLabel:  "+trueLabel)
    // trueLabel: Double = 0.0


    /*
    我们可以将RDD[Vector]整体作为输入做预测：
     */
    val predictions = lrModel.predict(data.map(lp => lp.features))
    predictions.take(5)
    // res1: Array[Double] = Array(1.0, 1.0, 1.0, 1.0, 1.0)


    /**
     * 5.5 评估分类模型的性能
     * 在使用模型做预测时，如何知道预测到底好不好呢？换句话说，应该知道怎么评估模型性能。
     * 通常在二分类中使用的评估方法包括：预测正确率和错误率、准确率和召回率、准确率-召回率
     * 曲线下方的面积、 ROC曲线、 ROC曲线下的面积和F-Measure。
     * 5.5.1 预测的正确率和错误率
     * 在二分类中，预测正确率可能是最简单评测方式，正确率等于训练样本中被正确分类的数目
     * 除以总样本数。类似地，错误率等于训练样本中被错误分类的样本数目除以总样本数。
     * 我们通过对输入特征进行预测并将预测值与实际标签进行比较，计算出模型在训练数据上的
     * 正确率。将对正确分类的样本数目求和并除以样本总数，得到平均分类正确率：
     */
    // compute accuracy for logistic regression
    val lrTotalCorrect = data.map { point =>
      if (lrModel.predict(point.features) == point.label) 1 else 0
    }.sum

    // lrTotalCorrect: Double = 3806.0

    // accuracy is the number of correctly classified examples (same as true label)
    // divided by the total number of examples
    val lrAccuracy = lrTotalCorrect / numData
    println("lrAccuracy:"+lrAccuracy)
    // lrAccuracy: Double = 0.5146720757268425
    /*
    我们得到了51.5%的正确率，结果看起来不是很好。我们的模型仅仅预测对了一半的训练数据，和随机猜测差不多。

    注意模型预测的值并不是恰好为1或0。预测的输出通常是实数，然后必须转
    换为预测类别。这是通过在分类器决策函数或打分函数中使用阈值来实现的。
    比如二分类的逻辑回归这个概率模型会在打分函数中返回类别为1的估计概
    率。因此典型的决策阈值是0.5。于是，如果类别1的概率估计超过50%，这个模
    型会将样本标记为类别1，否则标记为类别0。
    在一些模型中，阈值本身其实也可以作为模型参数进行调优。接下来我们将
    看到阈值在评估方法中也是很重要的。
     */


    // compute accuracy for the other models
    val svmTotalCorrect = data.map { point =>
      if (svmModel.predict(point.features) == point.label) 1 else 0
    }.sum
    val nbTotalCorrect = nbData.map { point =>
      if (nbModel.predict(point.features) == point.label) 1 else 0
    }.sum
    // decision tree threshold needs to be specified
    //注意，决策树的预测阈值需要明确给出，如下面加粗部分所示：

    val dtTotalCorrect = data.map { point =>
      val score = dtModel.predict(point.features)
      val predicted = if (score > 0.5) 1 else 0
      if (predicted == point.label) 1 else 0
    }.sum
    val svmAccuracy = svmTotalCorrect / numData
    println("svmAccuracy: "+svmAccuracy)
    // svmAccuracy: Double = 0.5146720757268425
    val nbAccuracy = nbTotalCorrect / numData
    println("nbAccuracy: "+nbAccuracy)

    // nbAccuracy: Double = 0.5803921568627451
    val dtAccuracy = dtTotalCorrect / numData
    println("dtAccuracy: "+dtAccuracy)
    // dtAccuracy: Double = 0.6482758620689655
    //对比发现， SVM和朴素贝叶斯模型性能都较差，而决策树模型正确率达65%，但还不是很高。


    /**
     * 5.5.2 准确率和召回率
     * 准确率-召回率（PR）曲线，表示给定模型随着决策阈值的改变，准确率和召回率的对应关系。
     * PR曲线下的面积为平均准确率。直觉上， PR曲线下的面积为1等价于一个完美模型，其准确率和召回率达到100%。
     *
     * 5.5.3 ROC曲线和AUC
     * ROC曲线在概念上和PR曲线类似，它是对分类器的真阳性率假阳性率的图形化解释。
     * 真阳性率（ TPR）是真阳性的样本数除以真阳性和假阴性的样本数之和。换句话说， TPR是
     * 真阳性数目占所有正样本的比例。这和之前提到的召回率类似，通常也称为敏感度。
     * 假阳性率（ FPR）是假阳性的样本数除以假阳性和真阴性的样本数之和。换句话说， FPR是
     * 假阳性样本数占所有负样本总数的比例。
     * 和准确率和召回率类似， ROC曲线（图5-9）表示了分类器性能在不同决策阈值下TPR对FPR
     * 的折衷。曲线上每个点代表分类器决策函数中不同的阈值。
     *
     * MLlib内置了一系列方法用来计算二分类的PR和ROC曲线下的面积。下面我们针对每一个模
     * 型来计算这些指标：
     */
    // compute area under PR and ROC curves for each model
    // generate binary classification metrics
    import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
    val metrics = Seq(lrModel, svmModel).map { model =>
      val scoreAndLabels = data.map { point =>
        (model.predict(point.features), point.label)
      }
      val metrics = new BinaryClassificationMetrics(scoreAndLabels)
      (model.getClass.getSimpleName, metrics.areaUnderPR, metrics.areaUnderROC)
    }


    /*
    我们之前已经训练朴素贝叶斯模型并计算准确率，其中使用的数据集是nbData版本，这里用同样的数据集计算分类的结果。
     */
    // again, we need to use the special nbData for the naive Bayes metrics
    val nbMetrics = Seq(nbModel).map{ model =>
      val scoreAndLabels = nbData.map { point =>
        val score = model.predict(point.features)
        (if (score > 0.5) 1.0 else 0.0, point.label)
      }
      val metrics = new BinaryClassificationMetrics(scoreAndLabels)
      (model.getClass.getSimpleName, metrics.areaUnderPR, metrics.areaUnderROC)
    }
    /*
    因为DecisionTreeModel模型没有实现其他三个模型都有的ClassificationModel接口，因此我们需要单独为这个模型编写如下代码计算结果：
     */
    // here we need to compute for decision tree separately since it does
    // not implement the ClassificationModel interface
    val dtMetrics = Seq(dtModel).map{ model =>
      val scoreAndLabels = data.map { point =>
        val score = model.predict(point.features)
        (if (score > 0.5) 1.0 else 0.0, point.label)
      }
      val metrics = new BinaryClassificationMetrics(scoreAndLabels)
      (model.getClass.getSimpleName, metrics.areaUnderPR, metrics.areaUnderROC)
    }
    val allMetrics = metrics ++ nbMetrics ++ dtMetrics
    allMetrics.foreach{ case (m, pr, roc) =>
      println(f"$m, Area under PR: ${pr * 100.0}%2.4f%%, Area under ROC: ${roc * 100.0}%2.4f%%")
    }
    /*
    LogisticRegressionModel, Area under PR: 75.6759%, Area under ROC: 50.1418%
    SVMModel, Area under PR: 75.6759%, Area under ROC: 50.1418%
    NaiveBayesModel, Area under PR: 68.0851%, Area under ROC: 58.3559%
    DecisionTreeModel, Area under PR: 74.3081%, Area under ROC: 64.8837%

    我们可以看到所有模型得到的平均准确率差不多。
    逻辑回归和SVM的AUC的结果在0.5左右，表明这两个模型并不比随机好。朴素贝叶斯模型
    和决策树模型性能稍微好些， AUC分别是0.58和0.65。但是，在二分类问题上这个性能并不是非常好
    */


    /**
     * 5.6 改进模型性能以及参数调优
     * 到底哪里出错了呢？为什么我们的模型如此复杂却只得到比随机稍好的结果？我们的模型
     * 哪里存在问题？
     * 想想看，我们只是简单地把原始数据送进了模型做训练。事实上，我们并没有把所有数据用
     * 在模型中，只是用了其中易用的数值部分。同时，我们也没有对这些数值特征做太多分析。
     * 5.6.1 特征标准化
     * 我们使用的许多模型对输入数据的分布和规模有着一些固有的假设，其中最常见的假设形式
     * 是特征满足正态分布。下面让我们进一步研究特征是如何分布的。
     * 具体做法，我们先将特征向量用RowMatrix类表示成MLlib中的分布矩阵。 RowMatrix是一个由向量组成的RDD，其中每个向量是分布矩阵的一行。
     * RowMatrix类中有一些方便操作矩阵的方法，其中一个方法可以计算矩阵每列的统计特性：
     */
    // standardizing the numerical features
    import org.apache.spark.mllib.linalg.distributed.RowMatrix
    val vectors = data.map(lp => lp.features)
    val matrix = new RowMatrix(vectors)
    val matrixSummary = matrix.computeColumnSummaryStatistics()

    println(matrixSummary.mean)
    // [0.41225805299526636,2.761823191986623,0.46823047328614004, ...
    println(matrixSummary.min)
    // [0.0,0.0,0.0,0.0,0.0,0.0,0.0,-1.0,0.0,0.0,0.0,0.045564223,-1.0, ...
    println(matrixSummary.max)
    // [0.999426,363.0,1.0,1.0,0.980392157,0.980392157,21.0,0.25,0.0,0.444444444, ...
    println(matrixSummary.variance)
    // [0.1097424416755897,74.30082476809638,0.04126316989120246, ...
    println(matrixSummary.numNonzeros)
    // [5053.0,7354.0,7172.0,6821.0,6160.0,5128.0,7350.0,1257.0,0.0,7362.0, ...

    /*
    computeColumnSummaryStatistics方法计算特征矩阵每列的不同统计数据，包括均值
    和方差，所有统计值按每列一项的方式存储在一个Vector中（在我们的例子中每个特征对应一项）。

    观察前面对均值和方差的输出，可以清晰发现第二个特征的方差和均值比其他的都要高（你
    会发现一些其他特征也有类似的结果，而且有些特征更加极端）。
    因为我们的数据在原始形式下，确切地说并不符合标准的高斯分布。为使数据更符合模型的假设，可以对每个特征进行标准化，
    使得每个特征是0均值和单位标准差。具体做法是对每个特征值减去列的均值，然后除以列的标准差以进行缩放：
    (x – μ) / sqrt(variance)
    实际上，我们可以对数据集中每个特征向量，与均值向量按项依次做减法，然后依次按项除
    以特征的标准差向量。标准差向量可以由方差向量的每项求平方根得到。
     */

    /**
     * 正如我们在第3章提到的，可以使用Spark的StandardScaler中的方法方便地完成这些操作。
     * StandardScaler工作方式和第3章的Normalizer特征有很多类似的地方。为了说清楚，我
     * 们传入两个参数，一个表示是否从数据中减去均值，另一个表示是否应用标准差缩放。这样使得
     * StandardScaler和我们的输入向量相符。最后，将输入向量传到转换函数，并且返回归一化的
     * 向量。具体实现代码如下，我们使用map函数来保留数据集的标签：
     */
    // scale the input features using MLlib's StandardScaler
    import org.apache.spark.mllib.feature.StandardScaler
    val scaler = new StandardScaler(withMean = true, withStd = true).fit(vectors)
    val scaledData = data.map(lp => LabeledPoint(lp.label, scaler.transform(lp.features)))

    //现在我们的数据被标准化后，观察第一行标准化前和标准化后的向量，下面输出第一行标准化前的特征向量：
    // compare the raw features with the scaled features
    println(data.first.features)
    // [0.789131,2.055555556,0.676470588,0.205882353,
    //下面输出第一行标准化后的特征向量：
    println(scaledData.first.features)
    // [1.1376439023494747,-0.08193556218743517,1.025134766284205,-0.0558631837375738,

    //可以看出，第一个特征已经应用标准差公式被转换了。为确认这一点，可以让第一个特征减
    //去其均值，然后除以标准差（方差的平方根）：
    println((0.789131 - 0.41225805299526636)/math.sqrt(0.1097424416755897))
    // 1.137647336497682
    //输出结果应该等于上面向量的第一个元素：

    //现在我们使用标准化的数据重新训练模型。这里只训练逻辑回归（因为决策树和朴素贝叶斯
    //不受特征标准话的影响），并说明特征标准化的影响：
    // train a logistic regression model on the scaled data, and compute metrics
    val lrModelScaled = LogisticRegressionWithSGD.train(scaledData, numIterations)
    val lrTotalCorrectScaled = scaledData.map { point =>
      if (lrModelScaled.predict(point.features) == point.label) 1 else 0
    }.sum
    val lrAccuracyScaled = lrTotalCorrectScaled / numData
    // lrAccuracyScaled: Double = 0.6204192021636241
    val lrPredictionsVsTrue = scaledData.map { point =>
      (lrModelScaled.predict(point.features), point.label)
    }
    val lrMetricsScaled = new BinaryClassificationMetrics(lrPredictionsVsTrue)
    val lrPr = lrMetricsScaled.areaUnderPR
    val lrRoc = lrMetricsScaled.areaUnderROC
    println(f"${lrModelScaled.getClass.getSimpleName}\nAccuracy: ${lrAccuracyScaled * 100}%2.4f%%\nArea under PR: ${lrPr * 100.0}%2.4f%%\nArea under ROC: ${lrRoc * 100.0}%2.4f%%")
    /*
    LogisticRegressionModel
    Accuracy: 62.0419%
    Area under PR: 72.7254%
    Area under ROC: 61.9663%

    从结果可以看出，通过简单对特征标准化，就提高了逻辑回归的准确率，并将AUC从随机50%提升到62%。
    */


    /**
     * 5.6.2 其他特征
     * 我们已经看到，需要注意对特征进行标准和归一化，这对模型性能可能有重要影响。在这个
     * 示例中，我们仅仅使用了部分特征，却完全忽略了类别（ category）变量和样板（ boilerplate）列的文本内容。
     * 这样做是为了便于介绍。现在我们再来评估一下添加其他特征，比如类别特征对性能的影响。
     * 首先，我们查看所有类别，并对每个类别做一个索引的映射，这里索引可以用于类别特征做
     * 1-of-k编码。
     */
    // Investigate the impact of adding in the 'category' feature
    val categories = records.map(r => r(3)).distinct.collect.zipWithIndex.toMap
    // categories: scala.collection.immutable.Map[String,Int] = Map("weather" -> 0, "sports" -> 6,
    //	"unknown" -> 4, "computer_internet" -> 12, "?" -> 11, "culture_politics" -> 3, "religion" -> 8,
    // "recreation" -> 2, "arts_entertainment" -> 9, "health" -> 5, "law_crime" -> 10, "gaming" -> 13,
    // "business" -> 1, "science_technology" -> 7)
    val numCategories = categories.size

    // numCategories: Int = 14
    //因此，我们需要创建一个长为14的向量来表示类别特征，然后根据每个样本所属类别索引，
    //对相应的维度赋值为1，其他为0。我们假定这个新的特征向量和其他的数值特征向量一样：
    val dataCategories = records.map { r =>
      val trimmed = r.map(_.replaceAll("\"", ""))
      val label = trimmed(r.size - 1).toInt
      val categoryIdx = categories(r(3))
      val categoryFeatures = Array.ofDim[Double](numCategories)
      categoryFeatures(categoryIdx) = 1.0
      val otherFeatures = trimmed.slice(4, r.size - 1).map(d => if (d == "?") 0.0 else d.toDouble)
      val features = categoryFeatures ++ otherFeatures
      LabeledPoint(label, Vectors.dense(features))
    }
    println(dataCategories.first)
    //你应该可以看到如下输出，其中第一部分是一个14维的向量，向量中类别对应索引那一维为1。
    // LabeledPoint(0.0, [0.0,1.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.789131,2.055555556,
    //	0.676470588,0.205882353,0.047058824,0.023529412,0.443783175,0.0,0.0,0.09077381,0.0,0.245831182,
    // 0.003883495,1.0,1.0,24.0,0.0,5424.0,170.0,8.0,0.152941176,0.079129575])


    //同样，因为我们的原始数据没有标准化，所以在训练这个扩展数据集之前，应该使用同样的
    //StandardScaler方法对其进行标准化转换：
    // standardize the feature vectors
    val scalerCats = new StandardScaler(withMean = true, withStd = true).fit(dataCategories.map(lp => lp.features))
    val scaledDataCats = dataCategories.map(lp => LabeledPoint(lp.label, scalerCats.transform(lp.features)))
    println(dataCategories.first.features)
    //可以使用如下代码看到标准化之前的特征：
    // [0.0,1.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.789131,2.055555556,0.676470588,0.205882353,
    // 0.047058824,0.023529412,0.443783175,0.0,0.0,0.09077381,0.0,0.245831182,0.003883495,1.0,1.0,24.0,0.0,
    // 5424.0,170.0,8.0,0.152941176,0.079129575]

    //可以使用如下代码看到标准化之后的特征：
    println(scaledDataCats.first.features)
    /**
     * 虽然原始特征是稀疏的（大部分维度是0），但对每个项减去均值之后，将得
     * 到一个非稀疏（稠密）的特征向量表示，如上面的例子所示。
     * 数据规模比较小的时候，稀疏的特征不会产生问题，但实践中往往大规模数
     * 据是非常稀疏的（比如在线广告和文本分类）。此时，不建议丢失数据的稀疏性，
     * 因为相应的稠密表示所需要的内存和计算量将爆炸性增长。这时我们可以将
     * StandardScaler的withMean设置为false来避免这个问题。
     */
    /*
    [-0.023261105535492967,2.720728254208072,-0.4464200056407091,-0.2205258360869135,-0.028492999745483565,
    -0.2709979963915644,-0.23272692307249684,-0.20165301179556835,-0.09914890962355712,-0.381812077600508,
    -0.06487656833429316,-0.6807513271391559,-0.2041811690290381,-0.10189368073492189,1.1376439023494747,
    -0.08193556218743517,1.0251347662842047,-0.0558631837375738,-0.4688883677664047,-0.35430044806743044
    ,-0.3175351615705111,0.3384496941616097,0.0,0.8288021759842215,-0.14726792180045598,0.22963544844991393,
    -0.14162589530918376,0.7902364255801262,0.7171932152231301,-0.29799680188379124,-0.20346153667348232,
    -0.03296720969318916,-0.0487811294839849,0.9400696843533806,-0.10869789547344721,-0.2788172632659348]
    */


    // train model on scaled data and evaluate metrics
    val lrModelScaledCats = LogisticRegressionWithSGD.train(scaledDataCats, numIterations)
    val lrTotalCorrectScaledCats = scaledDataCats.map { point =>
      if (lrModelScaledCats.predict(point.features) == point.label) 1 else 0
    }.sum
    val lrAccuracyScaledCats = lrTotalCorrectScaledCats / numData
    val lrPredictionsVsTrueCats = scaledDataCats.map { point =>
      (lrModelScaledCats.predict(point.features), point.label)
    }
    val lrMetricsScaledCats = new BinaryClassificationMetrics(lrPredictionsVsTrueCats)
    val lrPrCats = lrMetricsScaledCats.areaUnderPR
    val lrRocCats = lrMetricsScaledCats.areaUnderROC
    println(f"${lrModelScaledCats.getClass.getSimpleName}\nAccuracy: ${lrAccuracyScaledCats * 100}%2.4f%%\nArea under PR: ${lrPrCats * 100.0}%2.4f%%\nArea under ROC: ${lrRocCats * 100.0}%2.4f%%")
    /*
    LogisticRegressionModel
    Accuracy: 66.5720%
    Area under PR: 75.7964%
    Area under ROC: 66.5483%
    通过对数据的特征标准化，模型准确率得到提升，将AUC从50%提高到62%。之后，通过添
    加类别特征，模型性能进一步提升到66%（其中新添加的特征也做了标准化操作）。
    */
    /**
     * 竞赛中性能最好模型的AUC为0.889 06（ http://www.kaggle.com/c/stumbleupon/
     * leaderboard/private）。
     * 另一个性能几乎差不多高的在这里： http://www.kaggle.com/c/stumbleupon/
     * forums/t/5680/beating-the-benchmark- leaderboard-auc-0-878。
     * 需要指出的是，有些特征我们仍然没有用，特别是样板变量中的文本特征。
     * 竞赛中性能突出的模型主要使用了样板特征以及基于文本内容的特征来提升性
     * 能。从前面的实验可以看出，添加了类别特征提升性能之后，大部分变量用于预
     * 测都是没有用的，但是文本内容预测能力很强。
     * 通过对比赛中获得最好性能的方法进行学习，可以得到一些很好的启发，比
     * 如特征提取和特征工程对模型性能提升很重要
     */


    /**
     * 5.6.3 使用正确的数据格式
     * 模型性能的另外一个关键部分是对每个模型使用正确的数据格式。前面对数值向量应用朴素
     * 贝叶斯模型得到了非常差的结果，这难道是模型自身的缺陷？
     * 在这里，我们知道MLlib实现了多项式模型，并且该模型可以处理计数形式的数据。这包
     * 括二元表示的类型特征（比如前面提到的1-of-k表示）或者频率数据（比如一个文档中单词出
     * 现的频率）。我开始时使用的数值特征并不符合假定的输入分布，所以模型性能不好也并不是
     * 意料之外。
     * 为了更好地说明，我们仅仅使用类型特征，而1-of-k编码的类型特征更符合朴素贝叶斯模型，
     * 我们用如下代码构建数据集:
     */
    // train naive Bayes model with only categorical data
    val dataNB = records.map { r =>
      val trimmed = r.map(_.replaceAll("\"", ""))
      val label = trimmed(r.size - 1).toInt
      val categoryIdx = categories(r(3))
      val categoryFeatures = Array.ofDim[Double](numCategories)
      categoryFeatures(categoryIdx) = 1.0
      LabeledPoint(label, Vectors.dense(categoryFeatures))
    }
    val nbModelCats = NaiveBayes.train(dataNB)
    val nbTotalCorrectCats = dataNB.map { point =>
      if (nbModelCats.predict(point.features) == point.label) 1 else 0
    }.sum
    val nbAccuracyCats = nbTotalCorrectCats / numData
    val nbPredictionsVsTrueCats = dataNB.map { point =>
      (nbModelCats.predict(point.features), point.label)
    }
    val nbMetricsCats = new BinaryClassificationMetrics(nbPredictionsVsTrueCats)
    val nbPrCats = nbMetricsCats.areaUnderPR
    val nbRocCats = nbMetricsCats.areaUnderROC
    println(f"${nbModelCats.getClass.getSimpleName}\nAccuracy: ${nbAccuracyCats * 100}%2.4f%%\nArea under PR: ${nbPrCats * 100.0}%2.4f%%\nArea under ROC: ${nbRocCats * 100.0}%2.4f%%")
    /*
    NaiveBayesModel
    Accuracy: 60.9601%
    Area under PR: 74.0522%
    Area under ROC: 60.5138%
    可见，使用格式正确的输入数据后，朴素贝叶斯的性能从58%提高到了60%。
    */


    /**
     * 5.6.4 模型参数调优
     * 前几节展示了模型性能的影响因素：特征提取、特征选择、输入数据的格式和模型对数据分
     * 布的假设。但是到目前为止，我们对模型参数的讨论只是一笔带过，而实际上它对于模型性能影
     * 响很大。
     * MLlib默认的train方法对每个模型的参数都使用默认值。接下来让我们深入了解一下这些
     * 参数。
     * 1. 线性模型
     * 逻辑回归和SVM模型有相同的参数，原因是它们都使用随机梯度下降（ SGD）作为基础优化
     * 技术。不同点在于二者采用的损失函数不同。 MLlib中关于逻辑回归类的定义如下：
     * class LogisticRegressionWithSGD private (
     * private var stepSize: Double,
     * private var numIterations: Int,
     * private var regParam: Double,
     * private var miniBatchFraction: Double)
     * extends GeneralizedLinearAlgorithm[LogisticRegressionModel] ...
     * 可以看到， stepSize、 numIterations、 regParam和miniBatchFraction能通过参数
     * 传递到构造函数中。这些变量中除了regParam以外都和基本的优化技术相关。
     * 下面是逻辑回归实例化的代码，代码初始化了Gradient、 Updater和Optimizer，以及
     * Optimizer相关的参数（这里是GradientDescent）：
     * private val gradient = new LogisticGradient()
     * private val updater = new SimpleUpdater()
     * override val optimizer = new GradientDescent(gradient, updater)
     * .setStepSize(stepSize)
     * .setNumIterations(numIterations)
     * .setRegParam(regParam)
     * .setMiniBatchFraction(miniBatchFraction)
     * LogisticGradient建立了定义逻辑回归模型的逻辑损失函数。
     *
     * 对优化技巧的详细描述已经超出本书的范围， MLlib为线性模型提供了两个
     * 优化技术： SGD和L-BFGS。 L-BFGS通常来说更精确，要调的参数较少。
     * SGD是所有模型默认的优化技术，而L-BGFS只有逻辑回归在LogisticRegression WithLBFGS中使用。你可以动手实现并比较一下二者的不同。更
     * 多细节可以访问http://spark.apache.org/docs/latest/mllib-optimization.html。
     */

    //为了研究其他参数的影响，我们需要创建一个辅助函数在给定参数之后训练逻辑回归模型。
    //首先需要引入必要的类：
    // investigate the impact of model parameters on performance
    // create a training function
    import org.apache.spark.rdd.RDD
    import org.apache.spark.mllib.optimization.Updater
    import org.apache.spark.mllib.optimization.SimpleUpdater
    import org.apache.spark.mllib.optimization.L1Updater
    import org.apache.spark.mllib.optimization.SquaredL2Updater
    import org.apache.spark.mllib.classification.ClassificationModel

    // helper function to train a logistic regresson model
    def trainWithParams(input: RDD[LabeledPoint], regParam: Double, numIterations: Int, updater: Updater, stepSize: Double) = {
      val lr = new LogisticRegressionWithSGD
      lr.optimizer.setNumIterations(numIterations).setUpdater(updater).setRegParam(regParam).setStepSize(stepSize)
      lr.run(input)
    }
    // helper function to create AUC metric
    def createMetrics(label: String, data: RDD[LabeledPoint], model: ClassificationModel) = {
      val scoreAndLabels = data.map { point =>
        (model.predict(point.features), point.label)
      }
      val metrics = new BinaryClassificationMetrics(scoreAndLabels)
      (label, metrics.areaUnderROC)
    }

    // cache the data to increase speed of multiple runs agains the dataset
    scaledDataCats.cache
    // num iterations
    val iterResults = Seq(1, 5, 10, 50).map { param =>
      val model = trainWithParams(scaledDataCats, 0.0, param, new SimpleUpdater, 1.0)
      createMetrics(s"$param iterations", scaledDataCats, model)
    }
    iterResults.foreach { case (param, auc) => println(f"$param, AUC = ${auc * 100}%2.2f%%") }
    /*
    1 iterations, AUC = 64.97%
    5 iterations, AUC = 66.62%
    10 iterations, AUC = 66.55%
    50 iterations, AUC = 66.81%
    */

    // step size
    val stepResults = Seq(0.001, 0.01, 0.1, 1.0, 10.0).map { param =>
      val model = trainWithParams(scaledDataCats, 0.0, numIterations, new SimpleUpdater, param)
      createMetrics(s"$param step size", scaledDataCats, model)
    }
    stepResults.foreach { case (param, auc) => println(f"$param, AUC = ${auc * 100}%2.2f%%") }
    /*
    0.001 step size, AUC = 64.95%
    0.01 step size, AUC = 65.00%
    0.1 step size, AUC = 65.52%
    1.0 step size, AUC = 66.55%
    10.0 step size, AUC = 61.92%
    */

    // regularization
    val regResults = Seq(0.001, 0.01, 0.1, 1.0, 10.0).map { param =>
      val model = trainWithParams(scaledDataCats, param, numIterations, new SquaredL2Updater, 1.0)
      createMetrics(s"$param L2 regularization parameter", scaledDataCats, model)
    }
    regResults.foreach { case (param, auc) => println(f"$param, AUC = ${auc * 100}%2.2f%%") }
    /*
    0.001 L2 regularization parameter, AUC = 66.55%
    0.01 L2 regularization parameter, AUC = 66.55%
    0.1 L2 regularization parameter, AUC = 66.63%
    1.0 L2 regularization parameter, AUC = 66.04%
    10.0 L2 regularization parameter, AUC = 35.33%
    */

    // investigate decision tree
    import org.apache.spark.mllib.tree.impurity.Impurity
    import org.apache.spark.mllib.tree.impurity.Entropy
    import org.apache.spark.mllib.tree.impurity.Gini
    def trainDTWithParams(input: RDD[LabeledPoint], maxDepth: Int, impurity: Impurity) = {
      DecisionTree.train(input, Algo.Classification, impurity, maxDepth)
    }

    // investigate tree depth impact for Entropy impurity
    val dtResultsEntropy = Seq(1, 2, 3, 4, 5, 10, 20).map { param =>
      val model = trainDTWithParams(data, param, Entropy)
      val scoreAndLabels = data.map { point =>
        val score = model.predict(point.features)
        (if (score > 0.5) 1.0 else 0.0, point.label)
      }
      val metrics = new BinaryClassificationMetrics(scoreAndLabels)
      (s"$param tree depth", metrics.areaUnderROC)
    }
    dtResultsEntropy.foreach { case (param, auc) => println(f"$param, AUC = ${auc * 100}%2.2f%%") }
    /*
    1 tree depth, AUC = 59.33%
    2 tree depth, AUC = 61.68%
    3 tree depth, AUC = 62.61%
    4 tree depth, AUC = 63.63%
    5 tree depth, AUC = 64.88%
    10 tree depth, AUC = 76.26%
    20 tree depth, AUC = 98.45%
    */

    // investigate tree depth impact for Gini impurity
    val dtResultsGini = Seq(1, 2, 3, 4, 5, 10, 20).map { param =>
      val model = trainDTWithParams(data, param, Gini)
      val scoreAndLabels = data.map { point =>
        val score = model.predict(point.features)
        (if (score > 0.5) 1.0 else 0.0, point.label)
      }
      val metrics = new BinaryClassificationMetrics(scoreAndLabels)
      (s"$param tree depth", metrics.areaUnderROC)
    }
    dtResultsGini.foreach { case (param, auc) => println(f"$param, AUC = ${auc * 100}%2.2f%%") }
    /*
    1 tree depth, AUC = 59.33%
    2 tree depth, AUC = 61.68%
    3 tree depth, AUC = 62.61%
    4 tree depth, AUC = 63.63%
    5 tree depth, AUC = 64.89%
    10 tree depth, AUC = 78.37%
    20 tree depth, AUC = 98.87%
    */

    // investigate Naive Bayes parameters
    def trainNBWithParams(input: RDD[LabeledPoint], lambda: Double) = {
      val nb = new NaiveBayes
      nb.setLambda(lambda)
      nb.run(input)
    }
    val nbResults = Seq(0.001, 0.01, 0.1, 1.0, 10.0).map { param =>
      val model = trainNBWithParams(dataNB, param)
      val scoreAndLabels = dataNB.map { point =>
        (model.predict(point.features), point.label)
      }
      val metrics = new BinaryClassificationMetrics(scoreAndLabels)
      (s"$param lambda", metrics.areaUnderROC)
    }
    nbResults.foreach { case (param, auc) => println(f"$param, AUC = ${auc * 100}%2.2f%%") }
    /*
    0.001 lambda, AUC = 60.51%
    0.01 lambda, AUC = 60.51%
    0.1 lambda, AUC = 60.51%
    1.0 lambda, AUC = 60.51%
    10.0 lambda, AUC = 60.51%
    */

    // illustrate cross-validation
    // create a 60% / 40% train/test data split
    val trainTestSplit = scaledDataCats.randomSplit(Array(0.6, 0.4), 123)
    val train = trainTestSplit(0)
    val test = trainTestSplit(1)
    // now we train our model using the 'train' dataset, and compute predictions on unseen 'test' data
    // in addition, we will evaluate the differing performance of regularization on training and test datasets
    val regResultsTest = Seq(0.0, 0.001, 0.0025, 0.005, 0.01).map { param =>
      val model = trainWithParams(train, param, numIterations, new SquaredL2Updater, 1.0)
      createMetrics(s"$param L2 regularization parameter", test, model)
    }
    regResultsTest.foreach { case (param, auc) => println(f"$param, AUC = ${auc * 100}%2.6f%%") }
    /*
    0.0 L2 regularization parameter, AUC = 66.480874%
    0.001 L2 regularization parameter, AUC = 66.480874%
    0.0025 L2 regularization parameter, AUC = 66.515027%
    0.005 L2 regularization parameter, AUC = 66.515027%
    0.01 L2 regularization parameter, AUC = 66.549180%
    */

    // training set results
    val regResultsTrain = Seq(0.0, 0.001, 0.0025, 0.005, 0.01).map { param =>
      val model = trainWithParams(train, param, numIterations, new SquaredL2Updater, 1.0)
      createMetrics(s"$param L2 regularization parameter", train, model)
    }
    regResultsTrain.foreach { case (param, auc) => println(f"$param, AUC = ${auc * 100}%2.6f%%") }
/*
0.0 L2 regularization parameter, AUC = 66.260311%
0.001 L2 regularization parameter, AUC = 66.260311%
0.0025 L2 regularization parameter, AUC = 66.260311%
0.005 L2 regularization parameter, AUC = 66.238294%
0.01 L2 regularization parameter, AUC = 66.238294%
*/

    sc.stop()
  }
}

/*
   sed 的基本用法：
   删除某行
     [root@localhost ruby] # sed '1d' ab              #删除第一行
     [root@localhost ruby] # sed '$d' ab              #删除最后一行
     [root@localhost ruby] # sed '1,2d' ab           #删除第一行到第二行
     [root@localhost ruby] # sed '2,$d' ab           #删除第二行到最后一行

　　显示某行
.    [root@localhost ruby] # sed -n '1p' ab           #显示第一行
     [root@localhost ruby] # sed -n '$p' ab           #显示最后一行
     [root@localhost ruby] # sed -n '1,2p' ab        #显示第一行到第二行
     [root@localhost ruby] # sed -n '2,$p' ab        #显示第二行到最后一行

　　使用模式进行查询
     [root@localhost ruby] # sed -n '/ruby/p' ab    #查询包括关键字ruby所在所有行
     [root@localhost ruby] # sed -n '/\$/p' ab        #查询包括关键字$所在所有行，使用反斜线\屏蔽特殊含义

　　增加一行或多行字符串
     [root@localhost ruby]# cat ab
     Hello!
     ruby is me,welcome to my blog.
     end
     [root@localhost ruby] # sed '1a drink tea' ab  #第一行后增加字符串"drink tea"
     Hello!
     drink tea
     ruby is me,welcome to my blog.
     end
     [root@localhost ruby] # sed '1,3a drink tea' ab #第一行到第三行后增加字符串"drink tea"
     Hello!
     drink tea
     ruby is me,welcome to my blog.
     drink tea
     end
     drink tea
     [root@localhost ruby] # sed '1a drink tea\nor coffee' ab   #第一行后增加多行，使用换行符\n
     Hello!
     drink tea
     or coffee
     ruby is me,welcome to my blog.
     end

　　代替一行或多行
     [root@localhost ruby] # sed '1c Hi' ab                #第一行代替为Hi
     Hi
     ruby is me,welcome to my blog.
     end
     [root@localhost ruby] # sed '1,2c Hi' ab             #第一行到第二行代替为Hi
     Hi
     end

　　替换一行中的某部分
　　格式：sed 's/要替换的字符串/新的字符串/g'   （要替换的字符串可以用正则表达式）
     [root@localhost ruby] # sed -n '/ruby/p' ab | sed 's/ruby/bird/g'    #替换ruby为bird
　  [root@localhost ruby] # sed -n '/ruby/p' ab | sed 's/ruby//g'        #删除ruby

     插入
     [root@localhost ruby] # sed -i '$a bye' ab         #在文件ab中最后一行直接输入"bye"
     [root@localhost ruby]# cat ab
     Hello!
     ruby is me,welcome to my blog.
     end
     bye

     删除匹配行

      sed -i '/匹配字符串/d'  filename  （注：若匹配字符串是变量，则需要“”，而不是‘’。记得好像是）

      替换匹配行中的某个字符串

      sed -i '/匹配字符串/s/替换源字符串/替换目标字符串/g' filename

分类: Linux
 */