package org.training.spark.mllib

import org.apache.spark.{SparkConf, SparkContext}

/**
 *本代码为Spark机器学习书籍的第五章参考程序
 * 第5章 Spark构建分类模型
 */
object MovieLensClusterModel7 {

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
    var itemDataPath = "/Users/liulebin/Documents/codeing/codeingForSelfStudy/Spark_Study/Spark-learning/data/ml-100k/u.item"
    var genreDataPath = "/Users/liulebin/Documents/codeing/codeingForSelfStudy/Spark_Study/Spark-learning/data/ml-100k/u.genre"

    val conf = new SparkConf().setAppName("MovieLensRecommod")
    if(args.length > 0) {
      itemDataPath = args(0)
    } else {
      conf.setMaster("local[1]")
    }

    val sc = new SparkContext(conf)

    // load movie data
    val movies = sc.textFile(itemDataPath)
    println(movies.first)
    // 1|Toy Story (1995)|01-Jan-1995||http://us.imdb.com/M/title-exact?Toy%20Story%20(1995)|0|0|0|1|1|1|0|0|0|0|0|0|0|0|0|0|0|0|0
    val genres = sc.textFile(genreDataPath)
    genres.take(5).foreach(println)

    /*
    unknown|0
    Action|1
    Adventure|2
    Animation|3
    Children's|4
    */
    val genreMap = genres.filter(!_.isEmpty).map(line => line.split("\\|")).map(array => (array(1), array(0))).collectAsMap
    println(genreMap)
    // Map(2 -> Adventure, 5 -> Comedy, 12 -> Musical, 15 -> Sci-Fi, 8 -> Drama, 18 -> Western, ...

    val titlesAndGenres = movies.map(_.split("\\|")).map { array =>
      val genres = array.toSeq.slice(5, array.size)
      val genresAssigned = genres.zipWithIndex.filter { case (g, idx) =>
        g == "1"
      }.map { case (g, idx) =>
        genreMap(idx.toString)
      }
      (array(0).toInt, (array(1), genresAssigned))
    }
    println(titlesAndGenres.first)
    // (1,(Toy Story (1995),ArrayBuffer(Animation, Children's, Comedy)))

    // Run ALS model to generate movie and user factors
    import org.apache.spark.mllib.recommendation.ALS
    import org.apache.spark.mllib.recommendation.Rating
    val rawData = sc.textFile("/Users/liulebin/Documents/codeing/codeingForSelfStudy/Spark_Study/Spark-learning/data/ml-100k/u.data")
    val rawRatings = rawData.map(_.split("\t").take(3))
    val ratings = rawRatings.map{ case Array(user, movie, rating) => Rating(user.toInt, movie.toInt, rating.toDouble) }
    ratings.cache
    val alsModel = ALS.train(ratings, 50, 10, 0.1)

    // extract factor vectors
    import org.apache.spark.mllib.linalg.Vectors
    val movieFactors = alsModel.productFeatures.map { case (id, factor) => (id, Vectors.dense(factor)) }
    val movieVectors = movieFactors.map(_._2)
    val userFactors = alsModel.userFeatures.map { case (id, factor) => (id, Vectors.dense(factor)) }
    val userVectors = userFactors.map(_._2)

    // investigate distribution of features
    import org.apache.spark.mllib.linalg.distributed.RowMatrix
    val movieMatrix = new RowMatrix(movieVectors)
    val movieMatrixSummary = movieMatrix.computeColumnSummaryStatistics()
    val userMatrix = new RowMatrix(userVectors)
    val userMatrixSummary = userMatrix.computeColumnSummaryStatistics()
    println("Movie factors mean: " + movieMatrixSummary.mean)
    println("Movie factors variance: " + movieMatrixSummary.variance)
    println("User factors mean: " + userMatrixSummary.mean)
    println("User factors variance: " + userMatrixSummary.variance)
    // Movie factors mean: [0.28047737659519767,0.26886479057520024,0.2935579964446398,0.27821738264113755, ...
    // Movie factors variance: [0.038242041794064895,0.03742229118854288,0.044116961097355877,0.057116244055791986, ...
    // User factors mean: [0.2043520841572601,0.22135773814655782,0.2149706318418221,0.23647602029329481, ...
    // User factors variance: [0.037749421148850396,0.02831191551960241,0.032831876953314174,0.036775110657850954, ...

    // run K-means model on movie factor vectors
    import org.apache.spark.mllib.clustering.KMeans
    val numClusters = 5
    val numIterations = 10
    val numRuns = 3
    val movieClusterModel = KMeans.train(movieVectors, numClusters, numIterations, numRuns)
    /*
    ...
    14/09/02 22:16:45 INFO SparkContext: Job finished: collectAsMap at KMeans.scala:193, took 0.02043 s
    14/09/02 22:16:45 INFO KMeans: Iterations took 0.300 seconds.
    14/09/02 22:16:45 INFO KMeans: KMeans reached the max number of iterations: 10.
    14/09/02 22:16:45 INFO KMeans: The cost for the best run is 2585.6805358546403.
    ...
    movieClusterModel: org.apache.spark.mllib.clustering.KMeansModel = org.apache.spark.mllib.clustering.KMeansModel@2771ccdc
    */
    // convergence example
    val movieClusterModelConverged = KMeans.train(movieVectors, numClusters, 100)
    /*
    ...
    14/09/02 22:04:38 INFO SparkContext: Job finished: collectAsMap at KMeans.scala:193, took 0.040685 s
    14/09/02 22:04:38 INFO KMeans: Run 0 finished in 34 iterations
    14/09/02 22:04:38 INFO KMeans: Iterations took 0.812 seconds.
    14/09/02 22:04:38 INFO KMeans: KMeans converged in 34 iterations.
    14/09/02 22:04:38 INFO KMeans: The cost for the best run is 2584.9354332904104.
    ...
    movieClusterModelConverged: org.apache.spark.mllib.clustering.KMeansModel = org.apache.spark.mllib.clustering.KMeansModel@6bb28fb5
    */

    // train user model
    val userClusterModel = KMeans.train(userVectors, numClusters, numIterations, numRuns)

    // predict a movie cluster for movie 1
    val movie1 = movieVectors.first
    val movieCluster = movieClusterModel.predict(movie1)
    println(movieCluster)
    // 4
    // predict clusters for all movies
    val predictions = movieClusterModel.predict(movieVectors)
    println(predictions.take(10).mkString(","))
    // 0,0,1,1,2,1,0,1,1,1

    // inspect the movie clusters, by looking at the movies that are closest to each cluster center

    // define Euclidean distance function
    import breeze.linalg._
    import breeze.numerics.pow
    def computeDistance(v1: DenseVector[Double], v2: DenseVector[Double]): Double = pow(v1 - v2, 2).sum

    // join titles with the factor vectors, and compute the distance of each vector from the assigned cluster center
    val titlesWithFactors = titlesAndGenres.join(movieFactors)
    val moviesAssigned = titlesWithFactors.map { case (id, ((title, genres), vector)) =>
      val pred = movieClusterModel.predict(vector)
      val clusterCentre = movieClusterModel.clusterCenters(pred)
      val dist = computeDistance(DenseVector(clusterCentre.toArray), DenseVector(vector.toArray))
      (id, title, genres.mkString(" "), pred, dist)
    }
    val clusterAssignments = moviesAssigned.groupBy { case (id, title, genres, cluster, dist) => cluster }.collectAsMap

    for ( (k, v) <- clusterAssignments.toSeq.sortBy(_._1)) {
      println(s"Cluster $k:")
      val m = v.toSeq.sortBy(_._5)
      println(m.take(20).map { case (_, title, genres, _, d) => (title, genres, d) }.mkString("\n"))
      println("=====\n")
    }

    // clustering mathematical evaluation

    // compute the cost (WCSS) on for movie and user clustering
    val movieCost = movieClusterModel.computeCost(movieVectors)
    val userCost = userClusterModel.computeCost(userVectors)
    println("WCSS for movies: " + movieCost)
    println("WCSS for users: " + userCost)
    // WCSS for movies: 2586.0777166339426
    // WCSS for users: 1403.4137493396831

    // cross-validation for movie clusters
    val trainTestSplitMovies = movieVectors.randomSplit(Array(0.6, 0.4), 123)
    val trainMovies = trainTestSplitMovies(0)
    val testMovies = trainTestSplitMovies(1)
    val costsMovies = Seq(2, 3, 4, 5, 10, 20).map { k => (k, KMeans.train(trainMovies, numIterations, k, numRuns).computeCost(testMovies)) }
    println("Movie clustering cross-validation:")
    costsMovies.foreach { case (k, cost) => println(f"WCSS for K=$k id $cost%2.2f") }
    /*
    Movie clustering cross-validation:
    WCSS for K=2 id 942.06
    WCSS for K=3 id 942.67
    WCSS for K=4 id 950.35
    WCSS for K=5 id 948.20
    WCSS for K=10 id 943.26
    WCSS for K=20 id 947.10
    */

    // cross-validation for user clusters
    val trainTestSplitUsers = userVectors.randomSplit(Array(0.6, 0.4), 123)
    val trainUsers = trainTestSplitUsers(0)
    val testUsers = trainTestSplitUsers(1)
    val costsUsers = Seq(2, 3, 4, 5, 10, 20).map { k => (k, KMeans.train(trainUsers, numIterations, k, numRuns).computeCost(testUsers)) }
    println("User clustering cross-validation:")
    costsUsers.foreach { case (k, cost) => println(f"WCSS for K=$k id $cost%2.2f") }
    /*
    User clustering cross-validation:
    WCSS for K=2 id 544.02
    WCSS for K=3 id 542.18
    WCSS for K=4 id 542.38
    WCSS for K=5 id 542.33
    WCSS for K=10 id 539.68
    WCSS for K=20 id 541.21
    */

    sc.stop()
  }
}