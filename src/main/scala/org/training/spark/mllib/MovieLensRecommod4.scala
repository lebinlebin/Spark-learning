package org.training.spark.mllib

import java.util.Random

import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd._
import org.apache.spark.{SparkConf, SparkContext}

/**
 *本代码为Spark机器学习书籍的第四章参考程序
 * 第4章 构建基于Spark的推荐引擎
 */
object MovieLensRecommod4 {

  def main(args: Array[String]) {
    var dataPath = "data/mllib"
    val conf = new SparkConf().setAppName("MovieLensRecommod")
    if(args.length > 0) {
      dataPath = args(0)
    } else {
      conf.setMaster("local[1]")
    }

    val sc = new SparkContext(conf)
    val rawData = sc.textFile("/Users/liulebin/Documents/codeing/codeingForSelfStudy/Spark_Study/Spark-learning/data/ml-100k/u.data")
    println(rawData.first())

    /* Extract the user_id, movie_id and rating only from the dataset */
    val rawRatings = rawData.map(_.split("\t").take(3))
    println(rawRatings.first())
    // res25: Array[String] = Array(196, 242, 3)

    /* Import Spark's ALS recommendation model and inspect the train method */
    import org.apache.spark.mllib.recommendation.ALS
    //ALS.train()
    /*
    在终端上可以使用Tab键来查看ALS对象可用的函数有那些。输入ALS.（注意点号），然后按Tab键，应可看到如下自动完成功能所提示的函数项：
    ALS.
      asInstanceOf isInstanceOf main toString train trainImplicit
     这里要使用的函数是train。若只输入ALS.train 然后回车，终端会提示错误。但这个错误会包含该函数的声明信息：
      <console>:13: error: ambiguous reference to overloaded definition,
      both method train in object ALS of type (
      ratings: org.apache.spark.rdd.RDD[org.apache.spark.mllib.recommendation.Rating],
      rank: Int,
      iterations: Int)org.apache.spark.mllib.recommendation.MatrixFactorizationModel
      and  method train in object ALS of type (
      ratings: org.apache.spark.rdd.RDD[org.apache.spark.mllib.recommendation.Rating],
      rank: Int,
      iterations: Int,
      lambda: Double)org.apache.spark.mllib.recommendation.MatrixFactorizationModel
      match expected type ?
                    ALS.train
                        ^
      由此可知，所需提供的输入参数至少有ratings、 rank和iterations。第二个函数另外还
      需要一个lambda参数。先导入上面提到的Rating类，再类似地输入Rating()后回车，便可看
      到Rating对象所需的参数：
    */

    /* Import the Rating class and inspect it */
    import org.apache.spark.mllib.recommendation.Rating
//    Rating()
    /*
      <console>:13: error: not enough arguments for method apply:
      (user: Int, product: Int, rating: Double) org.apache.spark.mllib.recommendation.Rating in object Rating.
      Unspecified value parameters user, product, rating.
                    Rating()
                          ^
      上述输出表明ALS模型需要一个由Rating记录构成的RDD，
      而Rating类则是对用户ID、影片ID（这里是通称product）和实际星级这些参数的封装。
      我们可以调用map方法将原来的各ID和星级的数组转换为对应的Rating对象，从而创建所需的评级数据集。
    */


    /* Construct the RDD of Rating objects */
    val ratings = rawRatings.map { case Array(user, movie, rating) =>
      Rating(user.toInt, movie.toInt, rating.toDouble)
    }
    /*
    这里需要使用toInt或toDouble来将原始的评级数据（它从文本文件生成，类型为String）转换为Int或Double类型的数值输入。
    另外，这里还使用了case语句来提取各属性对应的变量名并直接使用它们。 （这样就不用使用val user = ratings(0)之类的表达。）
     */
    ratings.first()
    // res28: org.apache.spark.mllib.recommendation.Rating = Rating(196,242,3.0)

    /* Train the ALS model with rank=50, iterations=10, lambda=0.01 */
    val model = ALS.train(ratings, 50, 10, 0.01)
    // ...
    // 14/03/30 13:28:44 INFO MemoryStore: ensureFreeSpace(128) called with curMem=7544924, maxMem=311387750
    // 14/03/30 13:28:44 INFO MemoryStore: Block broadcast_120 stored as values to memory (estimated size 128.0 B, free 289.8 MB)
    // model: org.apache.spark.mllib.recommendation.MatrixFactorizationModel
    // = org.apache.spark.mllib.recommendation.MatrixFactorizationModel@7c7fbd3b
    /* Inspect the user factors */
    /**
     * 上述代码返回一个MatrixFactorizationModel对象。该对象将用户因子和物品因子分别
     * 保存在一个(id,factor)对类型的RDD中。它们分别称作 userFeatures和 productFeatures。
     * 比如输入：
     * model.userFeatures
     * 可以看到，各因子的类型为Array[Double]
     */
    model.userFeatures//不会立即运行。
    // res29: org.apache.spark.rdd.RDD[(Int, Array[Double])] = FlatMappedRDD[1099] at flatMap at ALS.scala:231

    /* Count user factors and force computation */
    model.userFeatures.count//action 操作立即运行
    // ...
    // 14/03/30 13:30:08 INFO SparkContext: Job finished: count at <console>:26, took 5.009689 s
    // res30: Long = 943//用户一共有943个;帖子一共1682个;可以在用户数据和电影数据中查看条数

    model.productFeatures.count
    // ...
    // 14/03/30 13:30:59 INFO SparkContext: Job finished: count at <console>:26, took 0.247783 s
    // res31: Long = 1682


    /* Make a prediction for a single user and movie pair */
    val predictedRating = model.predict(789, 123)
    println("predictedRating:  "+predictedRating)
    /*
     MatrixFactorizationModel.scala:46, took 0.023077 s
     predictedRating: Double = 3.128545693368485
     可以看到，该模型预测用户789对电影123的评级为3.12。

     predict函数同样可以以(user, item) ID对类型的RDD对象为输入，这时它将为每一对都
     生成相应的预测得分。我们可以借助这个函数来同时为多个用户和物品进行预测。
     要为某个用户生成前K个推荐物品，可借助MatrixFactorizationModel所提供的
     recommendProducts函数来实现。该函数需两个输入参数： user和num。其中user是用户ID，而num是要推荐的物品个数。
     返回值为预测得分最高的前num个物品。这些物品的序列按得分排序。该得分为相应的用户因子向量和各个物品因子向量的点积。
     */

    /* Make predictions for a single user across all movies */
    val userId = 789
    val K1 = 10
    val topKRecs = model.recommendProducts(userId, K1)
    println(topKRecs.mkString("\n"))
    /*
    Rating(789,715,5.931851273771102)
    Rating(789,12,5.582301095666215)
    Rating(789,959,5.516272981542168)
    Rating(789,42,5.458065302395629)
    Rating(789,584,5.449949837103569)
    Rating(789,750,5.348768847643657)
    Rating(789,663,5.30832117499004)
    Rating(789,134,5.278933936827717)
    Rating(789,156,5.250959077906759)
    Rating(789,432,5.169863417126231)
    */

    /*
    2. 检验推荐内容
      要直观地检验推荐的效果，可以简单比对下用户所评级过的电影的标题和被推荐的那些电影
      的电影名。首先，我们需要读入电影数据（这是在上一章探索过的数据集）。这些数据会导入为
      Map[Int, String]类型，即从电影ID到标题的映射：
    Load movie titles to inspect the recommendations */
    val movies = sc.textFile("/Users/liulebin/Documents/codeing/codeingForSelfStudy/Spark_Study/Spark-learning/data/ml-100k/u.item")
    val titles =
      movies.map(line => line.split("\\|").take(2))//取出前两列
      .map(array => (array(0).toInt, array(1)))
      .collectAsMap()

    titles(123)
    // res68: String = Frighteners, The (1996)


    /*
    对用户789，我们可以找出他所接触过的电影、给出最高评级的前10部电影及名称。具体实现时，
    可先用Spark的keyBy函数来从ratings RDD来创建一个键值对RDD。其主键为用户ID。
    然后利用lookup函数来只返回给定键值（即特定用户ID）对应的那些评级数据。
     */
    val moviesForUser: Seq[Rating] = ratings.keyBy(_.user).lookup(789)//获取每一个用户对应的电影列表,之后获取用户789对应的电影列表
    // moviesForUser: Seq[org.apache.spark.mllib.recommendation.Rating]
    // = WrappedArray(Rating(789,1012,4.0), Rating(789,127,5.0), Rating(789,475,5.0), Rating(789,93,4.0), ...
    // ...
    //来看下这个用户评价了多少电影。这会用到moviesForUser的size函数：
    println(moviesForUser.size)//查看用户789对应的电影集合的大小
    // 33
    // 可以看到，这个用户对33部电影做过评级。


    /*
    接下来，我们要获取评级最高的前10部电影。具体做法是利用Rating对象的rating属性
    来对moviesForUser集合进行排序并选出排名前10的评级（含相应电影ID）。之后以其为输入，
    借助titles映射为“(电影名称，具体评级)”形式。再将名称与具体评级打印出来：
     */
    // moviesForUser: Seq[org.apache.spark.mllib.recommendation.Rating]
    // = WrappedArray(Rating(789,1012,4.0), Rating(789,127,5.0), Rating(789,475,5.0), Rating(789,93,4.0), ...
    moviesForUser.sortBy(-_.rating).take(10).map(rating =>
      (titles(rating.product), rating.rating)).foreach(println)
    /*
    (Godfather, The (1972),5.0)
    (Trainspotting (1996),5.0)
    (Dead Man Walking (1995),5.0)
    (Star Wars (1977),5.0)
    (Swingers (1996),5.0)
    (Leaving Las Vegas (1995),5.0)
    (Bound (1996),5.0)
    (Fargo (1996),5.0)
    (Last Supper, The (1995),5.0)
    (Private Parts (1997),4.0)
    */


    /**
     * 现在看下对该用户的前10个推荐，并利用上述相同的方式来查看它们的电影名（注意这些推荐已排序）：
     *
     */
    topKRecs.map(rating => (titles(rating.product), rating.rating)).foreach(println)
    /*
    (To Die For (1995),5.931851273771102)
    (Usual Suspects, The (1995),5.582301095666215)
    (Dazed and Confused (1993),5.516272981542168)
    (Clerks (1994),5.458065302395629)
    (Secret Garden, The (1993),5.449949837103569)
    (Amistad (1997),5.348768847643657)
    (Being There (1979),5.30832117499004)
    (Citizen Kane (1941),5.278933936827717)
    (Reservoir Dogs (1992),5.250959077906759)
    (Fantasia (1940),5.169863417126231)
    */

    /**
     * 4.4.2 物品推荐
     * 物品推荐是为回答如下问题：给定一个物品，有哪些物品与它最相似？这里，相似的确切定
     * 义取决于所使用的模型。大多数情况下，相似度是通过某种方式比较表示两个物品的向量而得到
     * 的。常见的相似度衡量方法包括皮尔森相关系数（ Pearson correlation）、针对实数向量的余弦相
     * 似度（ cosine similarity）和针对二元向量的杰卡德相似系数（ Jaccard similarity）。
     *
     * 1. 从MovieLens 100k数据集生成相似电影
     * MatrixFactorizationModel当前的API不能直接支持物品之间相似度的计算。所以我们
     * 要自己实现。
     *
     * 这里会使用余弦相似度来衡量相似度。另外采用jblas线性代数库（ MLlib的依赖库之一）来
     * 求向量点积。这些和现有的predict和recommendProducts函数的实现方式类似，但我们会用
     * 到余弦相似度而不仅仅只是求点积。
     * 我们想利用余弦相似度来对指定物品的因子向量与其他物品的做比较。进行线性计算时，除
     * 了因子向量外，还需要创建一个Array[Double]类型的向量对象。以该类型对象为构造函数的
     * 输入来创建一个jblas.DoubleMatrix类型对象的方法如下：
     *
     * 注意，使用jblas时，向量和矩阵都表示为一个DoubleMatrix类对象，但前者的是一维的而后者为二维的。
     */
    /* Compute item-to-item similarities between an item and the other items */
    import org.jblas.DoubleMatrix
    val aMatrix = new DoubleMatrix(Array(1.0, 2.0, 3.0))
    println("aMatrix: "+aMatrix)
    // aMatrix: org.jblas.DoubleMatrix = [1.000000; 2.000000; 3.000000]


    /*
    定义一个函数来计算两个向量之间的余弦相似度。余弦相似度是两个向量在n维空
    间里两者夹角的度数。它是两个向量的点积与各向量范数（或长度）的乘积的商。（余弦相似度
    用的范数为L2-范数， L2-norm。）这样，余弦相似度是一个正则化了的点积。
    该相似度的取值在-1到1之间。 1表示完全相似， 0表示两者互不相关（即无相似性）。这种衡
    量方法很有帮助，因为它还能捕捉负相关性。也就是说，当为1时则不仅表示两者不相关，还表
    示它们完全不同。
    Compute the cosine similarity between two vectors */
    def cosineSimilarity(vec1: DoubleMatrix, vec2: DoubleMatrix): Double = {
      vec1.dot(vec2) / (vec1.norm2() * vec2.norm2())
    }
    // cosineSimilarity: (vec1: org.jblas.DoubleMatrix, vec2: org.jblas.DoubleMatrix)Double


    /*
    下面以物品567为例从模型中取回其对应的因子。这可以通过调用lookup函数来实现。之前
    曾用过该函数来取回特定用户的评级信息。下面的代码中还使用了head函数。 lookup函数返回
    了一个数组而我们只需第一个值（实际上，数组里也只会有一个值，也就是该物品的因子向量）。
    这个因子的类型为Array[Double]，所以后面会用它来创建一个Double[Matrix]对象，
    然后再用该对象来计算它与自己的相似度：
     */
    val itemId = 567
    val itemFactor = model.productFeatures.lookup(itemId).head
    // itemFactor: Array[Double] = Array(0.15179359424040248, -0.2775955241896113, 0.9886005994661484, ...
    val itemVector = new DoubleMatrix(itemFactor)
    // itemVector: org.jblas.DoubleMatrix = [0.151794; -0.277596; 0.988601; -0.464013; 0.188061; 0.090506; ...
    cosineSimilarity(itemVector, itemVector)
    // res113: Double = 1.0000000000000002

    val sims = model.productFeatures.map{ case (id, factor) =>
      val factorVector = new DoubleMatrix(factor)
      val sim = cosineSimilarity(factorVector, itemVector)
      (id, sim)
    }

    /*
     接下来，对物品按照相似度排序，然后取出与物品567最相似的前10个物品：
     上述代码里使用了Spark的top函数。相比使用collect函数将结果返回驱动程序然后再本地
     排序，它能分布式计算出“前K个”结果，因而更高效。（注意，推荐系统要处理的用户和物品数目可能数以百万计。）
     Spark需要知道如何对sims RDD里的(item id, similarity score)对排序。为此，我
     们另外传入了一个参数给top函数。这个参数是一个Scala Ordering对象，它会告诉Spark根据键
     值对里的值排序（也就是用similarity排序）。
     */
    val sortedSims = sims.top(K1)(Ordering.by[(Int, Double), Double] {case (id, similarity) => similarity })

    // sortedSims: Array[(Int, Double)] = Array((567,1.0), (672,0.483244928887981), (1065,0.43267674923450905), ...
    println(sortedSims.mkString("\n"))
    /*
    很正常，排名第一的最相似物品就是我们给定的物品。之后便是以相似度排序的其他类似物品
    (567,1.0000000000000002)
    (1471,0.6932331537649621)
    (670,0.6898690594544726)
    (201,0.6897964975027041)
    (343,0.6891221044611473)
    (563,0.6864214133620066)
    (294,0.6812075443259535)
    (413,0.6754663844488256)
    (184,0.6702643811753909)
    (109,0.6594872765176396)
    */

    /*
    2. 检查推荐的相似物品来看下我们所给定的电影的名称是什么：
    We can check the movie title of our chosen movie and the most similar movies to it */
    println(titles(itemId))
    // Wes Craven's New Nightmare (1994)

    /*
    如在用户推荐中所做过的，我们可以看看推荐的那些电影名称是什么，从而直观上检查一下
    基于物品推荐的结果。这一次我们取前11部最相似电影，以排除给定的那部。所以，可以选取列
    表中的第1到11项：
     */
    val sortedSims2 = sims.top(K1 + 1)(Ordering.by[(Int, Double), Double] { case (id, similarity) => similarity })
    sortedSims2.slice(1, 11).map{ case (id, sim) => (titles(id), sim) }.mkString("\n")
    /*
    因为模型的初始化是随机的，这里显示的结果可能与你运行得到的结果有所不同。
    (Hideaway (1995),0.6932331537649621)
    (Body Snatchers (1993),0.6898690594544726)
    (Evil Dead II (1987),0.6897964975027041)
    (Alien: Resurrection (1997),0.6891221044611473)
    (Stephen King's The Langoliers (1995),0.6864214133620066)
    (Liar Liar (1997),0.6812075443259535)
    (Tales from the Crypt Presents: Bordello of Blood (1996),0.6754663844488256)
    (Army of Darkness (1993),0.6702643811753909)
    (Mystery Science Theater 3000: The Movie (1996),0.6594872765176396)
    (Scream (1996),0.6538249646863378)
    */

    /**
     * 4.5 推荐模型效果的评估
     * 4.5.1 均方差
     * 均方差（ Mean Squared Error， MSE）直接衡量“用户-物品”评级矩阵的重建误差。它也是
     * 一些模型里所采用的最小化目标函数，特别是许多矩阵分解类方法，比如ALS。因此，它常用于显式评级的情形.
     * 它的定义为各平方误差的和与总数目的商。其中平方误差是指预测到的评级与真实评级的差值的平方。
     * 下面以用户789为例做讲解。现在从之前计算的moviesForUser这个Ratings集合里找出该用户的第一个评级
     */
    /* Compute squared error between a predicted and actual rating */
    // We'll take the first rating for our example user 789
    val actualRating: Rating = moviesForUser.take(1)(0)//用户看的第一个电影及评分, Rating(789,1012,4.0)
    // actualRating: Seq[org.apache.spark.mllib.recommendation.Rating] = WrappedArray(Rating(789,1012,4.0))

    //可以看到该用户对该电影的评级为4。然后，求模型的预计评级：
    val predictedRating1 = model.predict(789, actualRating.product)//验证是否为4.0
    // ...
    // 14/04/13 13:01:15 INFO SparkContext: Job finished:
    // lookup at MatrixFactorizationModel.scala:46, took 0.025404 s
    // predictedRating1: Double = 4.001005374200248

    //可以看出模型预测的评级差不多也是4，十分接近用户的实际评级。最后，我们计算实际评级和预计评级的平方误差：
    val squaredError = math.pow(predictedRating1 - actualRating.rating, 2.0)
    println(squaredError)
    // squaredError: Double = 1.010777282523947E-6



    /*
    要计算整个数据集上的MSE，需要对每一条(user, movie, actual rating, predicted rating)记录都计算该平均误差，
    然后求和，再除以总的评级次数。具体实现如下：
    首先从ratings RDD里提取用户和物品的ID，并使用model.predict来对各个“用户-物品”对做预测。
    所得的RDD以“用户和物品ID”对作为主键，对应的预计评级作为值：
    Compute Mean Squared Error across the dataset */
    // Below code is taken from the Apache Spark MLlib guide at:
    // http://spark.apache.org/docs/latest/mllib-guide.html#collaborative-filtering-1
    val usersProducts = ratings.map{ case Rating(user, product, rating)  => (user, product)}
    val predictions: RDD[((Int, Int), Double)] = model.predict(usersProducts).map{
      case Rating(user, product, rating) => ((user, product), rating)
    }

    /*
    接着提取出真实的评级。同时，对ratings RDD做映射以让“用户物品”对为主键，实际
    评级为对应的值。这样，就得到了两个主键组成相同的RDD。将两者连接起来，以创建一个新的
    RDD。 这个RDD的主键为“用户-物品”对，键值为相应的实际评级和预计评级。
     */
    val ratingsAndPredictions = ratings.map{
      case Rating(user, product, rating) => ((user, product), rating)
    }.join(predictions) //join之后,除了key之外拼接上的数据为一个tuple (rating,predicted)

    /*
    最后，求上述MSE。具体先用reduce来对平方误差求和，然后再除以count函数所求得的总记录数：
     */
    val MSE = ratingsAndPredictions.map{
      case ((user, product), (actual, predicted)) =>  math.pow((actual - predicted), 2)
    }.reduce(_ + _) / ratingsAndPredictions.count
    println("Mean Squared Error = " + MSE)
    // ...
    // 14/04/13 15:29:21 INFO SparkContext: Job finished: count at <console>:31, took 0.538683 s
    // Mean Squared Error = 0.08231947642632856

    /*
    均方根误差（ Root Mean Squared Error， RMSE）的使用也很普遍，其计算只需在MSE上取平
    方根即可。这不难理解，因为两者背后使用的数据（即评级数据）相同。它等同于求预计评级和
    实际评级的差值的标准差。如下代码便可求出：
     */
    val RMSE = math.sqrt(MSE)
    println("Root Mean Squared Error = " + RMSE)
    // Root Mean Squared Error = 0.28691370902473196


    /**
     * 4.5.2 K值平均准确率
     * K值平均准确率（ MAPK）的意思是整个数据集上的K值平均准确率（ Average Precision at K metric， APK） 的均值。
     * APK是信息检索中常用的一个指标。它用于衡量针对某个查询所返回的“前K个”文档的平均相关性。
     * 对于每次查询，我们会将结果中的前K个与实际相关的文档进行比较。
     *
     * 用APK指标计算时，结果中文档的排名十分重要。如果结果中文档的实际相关性越高且排名
     * 也更靠前，那APK分值也就越高。由此，它也很适合评估推荐的好坏。因为推荐系统也会计算“前K个”推荐物，
     * 然后呈现给用户。如果在预测结果中得分更高（在推荐列表中排名也更靠前）的
     * 物品实际上也与用户更相关，那自然这个模型就更好。
     * APK和其他基于排名的指标同样也更适合评估隐式数据集上的推荐。这里用MSE相对就不那么合适。
     *
     * 当用APK来做评估推荐模型时，每一个用户相当于一个查询，而每一个“前K个”推荐物组
     * 成的集合则相当于一个查到的文档结果集合。用户对电影的实际评级便对应着文档的实际相关
     * 性。这样， APK所试图衡量的是模型对用户感兴趣和会去接触的物品的预测能力。
     *
     * 以下计算平均准确率的代码基于https://github.com/benhamner/Metrics。
     * 关 于 MAPK 的 更 多 信 息 可 参 见 https://www.kaggle.com/wiki/MeanAverage
     * Precision。
     */
    /* Compute Mean Average Precision at K */
    /* Function to compute average precision given a set of actual and predicted ratings */
    // Code for this function is based on: https://github.com/benhamner/Metrics
    def avgPrecisionK(actual: Seq[Int], predicted: Seq[Int], k: Int): Double = {
      val predK = predicted.take(k)
      var score = 0.0
      var numHits = 0.0
      for ((p, i) <- predK.zipWithIndex) {
        if (actual.contains(p)) {
          numHits += 1.0
          score += numHits / (i.toDouble + 1.0)
        }
      }
      if (actual.isEmpty) {
        1.0
      } else {
        score / scala.math.min(actual.size, k).toDouble
      }
    }
    /*
    可以看到，该函数包括两个数组。一个以各个物品及其评级为内容，另一个以模型所预测的物品及其评级为内容。
    下面来计算对用户789推荐的APK指标怎么样。首先提取出用户实际评级过的电影的ID：
     */
    val actualMovies = moviesForUser.map(_.product)
    // actualMovies: Seq[Int] = ArrayBuffer(1012, 127, 475, 93, 1161, 286, 293, 9, 50, 294, 181, 1, 1008, 508, 284, 1017, 137, 111, 742, 248, 249, 1007, 591, 150, 276, 151, 129, 100, 741, 288, 762, 628, 124)
    val predictedMovies = topKRecs.map(_.product)
    // predictedMovies: Array[Int] = Array(27, 497, 633, 827, 602, 849, 401, 584, 1035, 1014)
    val apk10 = avgPrecisionK(actualMovies, predictedMovies, 10)
    println("apk10: "+apk10)
    // apk10: Double = 0.0
    /*
    这里， APK的得分为0，这表明该模型在为该用户做相关电影预测上的表现并不理想。
    全局MAPK的求解要计算对每一个用户的APK得分，再求其平均。这就要为每一个用户都生
    成相应的推荐列表。针对大规模数据处理时，这并不容易，但我们可以通过Spark将该计算分布
    式进行。不过，这就会有一个限制，即每个工作节点都要有完整的物品因子矩阵。这样它们才能
    独立地计算某个物品向量与其他所有物品向量之间的相关性。然而当物品数量众多时，单个节点
    的内存可能保存不下这个矩阵。此时，这个限制也就成了问题。

    事实上并没有其他简单的途径来应对这个问题。一种可能的方式是只计算与
    所有物品中的一部分物品的相关性。这可通过局部敏感哈希算法（ Locality
    Sensitive Hashing）等来实现： http://en.wikipedia.org/wiki/Locality-sensitive_hashing。
     */


    /*
    Compute recommendations for all users
    首先，取回物品因子向量并用它来构建一个DoubleMatrix对象：
     */
    val itemFactors: Array[Array[Double]] = model.productFeatures.map { case (id, factor) => factor }.collect()
    val itemMatrix = new DoubleMatrix(itemFactors)
    println(itemMatrix.rows, itemMatrix.columns)
    // (1682,50)

    /*
    这说明itemMatrix的行列数分别为1682和50。这正常，因为电影数目和因子维数分别就
    是这么多。接下来，我们将该矩阵以一个广播变量的方式分发出去，以便每个工作节点都能访问到：
     */
    // broadcast the item factor matrix
    val imBroadcast = sc.broadcast(itemMatrix)

    // compute recommendations for each user, and sort them in order of score so that the actual input
    // for the APK computation will be correct
    /*
    现在可以计算每一个用户的推荐。这会对每一个用户因子进行一次map操作。在这个操作里，
    会对用户因子矩阵和电影因子矩阵做乘积，其结果为一个表示各个电影预计评级的向量（长度为1682，即电影的总数目）。
    之后，用预计评级对它们排序：
     */
    val allRecs = model.userFeatures.map{case (userId, array) =>
      val userVector = new DoubleMatrix(array)
      val scores: DoubleMatrix = imBroadcast.value.mmul(userVector)//每个用户的向量与每个帖子进行dot 相乘
      val sortedWithId = scores.data.zipWithIndex.sortBy(-_._1)
      val recommendedIds: Seq[Int] = sortedWithId.map(_._2 + 1).toSeq
      (userId, recommendedIds)
    }

    //这样就有了一个由每个用户ID及各自相对应的电影ID列表构成的RDD。这些电影ID按照预
    //计评级的高低排序。
    /*
     还需要每个用户对应的一个电影ID列表作为传入到APK函数的actual参数。我们已经有
     ratings RDD，所以只需从中提取用户和电影的ID即可。
     使用Spark的groupBy操作便可得到一个新RDD。该RDD包含每个用户ID所对应的(userid，movieid)
     对（因为groupBy操作所用的主键就是用户ID)
     */
    // next get all the movie ids per user, grouped by user id
    val userMovies = ratings.map{ case Rating(user, product, rating) => (user, product) }.groupBy(_._1)
    // userMovies: org.apache.spark.rdd.RDD[(Int, Seq[(Int, Int)])] = MapPartitionsRDD[277] at groupBy at <console>:21

    /*
    最后，可以通过Spark的jion操作将这两个RDD以用户ID相连接。这样，对于每一个用户，
    我们都有一个实际和预测的那些电影的ID。这些ID可以作为APK函数的输入。与计算MSE时类
    似，我们调用reduce操作来对这些APK得分求和，然后再除以总的用户数目（即allRecs RDD的大小）：
     */
    // finally, compute the APK for each user, and average them to find MAPK
    val K = 10
    val MAPK = allRecs.join(userMovies).map{ case (userId, (predicted, actualWithIds)) =>
      val actual = actualWithIds.map(_._2).toSeq
      avgPrecisionK(actual, predicted, K)
    }.reduce(_ + _) / allRecs.count
    println("Mean Average Precision at K = " + MAPK)
    // Mean Average Precision at K = 0.030486963254725705
    /*
    我们模型的MAPK得分相当低。但请注意，推荐类任务的这个得分通常都较低，特别是当物品的数量极大时。
    试着给lambda和rank设置其他的值，看一下你能否找到一个RMSE和MAPK得分更好的模型。
     */



    /*
    4.5.3 使用MLlib内置的评估函数
    前面我们从零开始对模型进行了MSE、 RMSE和MAPK三方面的评估。这是一段很有用的练
    习。同样， MLlib下的RegressionMetrics和RankingMetrics类也提供了相应的函数以方便
    模型评估。
    1. RMSE和MSE
    首先，我们使用RegressionMetrics来求解MSE和RMSE得分。实例化一个RegressionMetrics对象需要一个键值对类型的RDD。
    其每一条记录对应每个数据点上相应的预测值与实际
    值。代码实现如下。这里仍然会用到之前已经算出的ratingsAndPredictions RDD：
    Using MLlib built-in metrics */

    // MSE, RMSE and MAE
    import org.apache.spark.mllib.evaluation.RegressionMetrics
    val predictedAndTrue = ratingsAndPredictions.map {
      case ((user, product), (actual, predicted)) =>
      (actual, predicted)
    }
    val regressionMetrics = new RegressionMetrics(predictedAndTrue)
    /*
    之后就可以查看各种指标的情况，包括MSE和RMSE。下面将这些指标打印出来：
     */
    println("Mean Squared Error = " + regressionMetrics.meanSquaredError)
    println("Root Mean Squared Error = " + regressionMetrics.rootMeanSquaredError)
    // Mean Squared Error = 0.08231947642632852
    // Root Mean Squared Error = 0.2869137090247319

    /*
    2. MAP
    与计算MSE和RMSE一样，可以使用MLlib的RankingMetrics类来计算基于排名的评估指
    标。类似地，需要向我们之前的平均准确率函数传入一个键值对类型的RDD。其键为给定用户预
    测的推荐物品的ID数组，而值则是实际的物品ID数组。
    RankingMetrics中的K值平均准确率函数的实现与我们的有所不同，因而结果会不同。但
    全局平均准确率（ Mean Average Precision， MAP，并不设定阈值K）会和当K值较大（比如设为
    总的物品数目）时我们模型的计算结果相同
    首先，使用RankingMetrics来计算MAP：
     */
    // MAPK
    import org.apache.spark.mllib.evaluation.RankingMetrics
    val predictedAndTrueForRanking = allRecs.join(userMovies).map{
      case (userId, (predicted, actualWithIds)) =>
      val actual = actualWithIds.map(_._2)
      (predicted.toArray, actual.toArray)
    }
    val rankingMetrics = new RankingMetrics(predictedAndTrueForRanking)
    println("Mean Average Precision = " + rankingMetrics.meanAveragePrecision)
    // Mean Average Precision = 0.07171412913757183
    //接下来用和之前完全相同的方法来计算MAP，但是将K值设到很高，比如2000：
    // Compare to our implementation, using K = 2000 to approximate the overall MAP
    val MAPK2000 = allRecs.join(userMovies).map{ case (userId, (predicted, actualWithIds)) =>
      val actual = actualWithIds.map(_._2).toSeq
      avgPrecisionK(actual, predicted, 2000)
    }.reduce(_ + _) / allRecs.count
    println("Mean Average Precision = " + MAPK2000)
    // Mean Average Precision = 0.07171412913757186
    //你会发现，用这种方法计算得到的MAP与使用RankingMetrics计算得出的MAP相同：
    sc.stop()
  }
}