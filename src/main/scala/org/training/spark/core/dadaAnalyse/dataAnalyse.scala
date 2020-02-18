package org.training.spark.core.dadaAnalyse

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 看过“Lord of the Rings, The (1978)”用户的年龄和性别分布
 */
object dataAnalyse {
  /**
   * 计算中位数的方法
   *
   * @param spark    SparkSession
   * @param df       DataFrame
   * @param col_name the col which to calculate the middle
   * @return middle
   */
  def calc_middle(spark: SparkSession, df: DataFrame, col_name: String): Double = {
    import org.apache.spark.sql.functions._
    import spark.implicits._
    //统计总行数
    val c = df.count()
    println("##########################")
    df.show()
    val df_add_id = df
    if (c % 2 == 0) { // 如果是偶数
      val bf = df_add_id.select(col_name).where($"rank_id" === c / 2)
        .collect()(0).get(0).toString.trim.toDouble
      val af = df_add_id.select(col_name).where($"rank_id" === c / 2 + 1)
        .collect()(0).get(0).toString.trim.toDouble
      (bf + af) / 2
    } else {
      df_add_id.select(col_name)
        .where($"rank_id" === (c + 1) / 2)
        .collect()(0).get(0).toString.trim.toDouble
    }
  }

  def main(args: Array[String]) {

    var dataPath = "data/ml-1m"
    val conf = new SparkConf().setAppName("dataAnalyzer")
    if(args.length > 0) {
      dataPath = args(0)//如果程序命令行有输入路径就用命令行输入的地址为路径
    } else {
      conf.setMaster("local[1]")
    }

    val spark = SparkSession
        .builder()
        .config(conf)
        .getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._
    import org.apache.spark.sql.functions._
    val df = spark.read.textFile("/Users/liulebin/Documents/codeing/codeingForSelfStudy/Spark_Study/Spark-learning/data/dataAnaylse/dataAnalyse")
      .map(_.split(","))
      .map(x => (x(0), x(1)))
      .toDF("col1", "col2")
      .cache()
    df.describe().show()

    val dfzhongshuOS = spark.read.textFile("/Users/liulebin/Documents/codeing/codeingForSelfStudy/Spark_Study/Spark-learning/data/dataAnaylse/zhongweishuOushu.data")
      .map(_.split(","))
      .map(x => (x(0), x(1)))
      .toDF("col1", "col2")
      .cache()

    val dfzhongshuJS = spark.read.textFile("/Users/liulebin/Documents/codeing/codeingForSelfStudy/Spark_Study/Spark-learning/data/dataAnaylse/zhongweishuJishu.data")
      .map(_.split(","))
      .map(x => (x(0), x(1)))
      .toDF("col1", "col2")
      .cache()



    val buyer_bid = spark.read.textFile("/Users/liulebin/Documents/codeing/codeingForSelfStudy/Spark_Study/Spark-learning/data/dataAnaylse/buyer_bid")
      .map(_.split("\\t"))
      .map(x => (x(0), x(1)))
      .toDF("buyer_bid", "rank_bid")
      .cache()
    println("###################################")
    buyer_bid.show()
    buyer_bid.createOrReplaceTempView("jxbidview1")

    /**
     * 不推荐的方式
     * 得到col1列的均值怎么算？
     * 这时候可以借助collect算子来实现：
     */
    val col1_mean1 = df.describe("col1").collect()(1).get(1)
    println(col1_mean1)

    /**
     * 推荐的方式
     */
    import org.apache.spark.sql.functions._
    df.agg(mean($"col1").alias("col1_mean")).show()

    /**
     * 同理，要得到这个值，需要借助collect算子
     */
    import org.apache.spark.sql.functions._
    val col1_mean = df.agg(mean($"col1").alias("col1_mean")).collect()(0).get(0)
    println(col1_mean)

    /**
     * 求众数
     * 定义：是一组数据中出现次数最多的数值，叫众数
     */

    val v1 = df.select("col1")
      .groupBy("col1")
      .count()
      .orderBy(df("col1").desc)
      .collect()(0).get(0)
    println(v1)

    /**
     * 当然也可以用sql：
     */

    df.createTempView(viewName = "view1")
    import spark.sql
    val v2 = sql(sqlText = "select col1,count(*) as ct1 from view1 group by col1 order by ct1 desc")
      .select("col1").collect()//(1).get(0)
    println(">>>>>>>>>>>众数")
//    v2.show()
    println(v2.length)
//
    for(i <- 0 until v2.length){
      println(v2(i).get(0))
    }
//    println(v2)


    /**
     * 按照bid从小到大排序，之后按照区间分段，统计各个区间段内的的count
     * 这里其实等距频分段，每个bin下的数据条数都是固定的。
     */
    val bid_block = spark.sql(
      """
        |SELECT
        | CEIL(rank_id/(all_count/5)) as bin_id,
        | AVG(buyer_bid) as avg_buyer_bid,
        | min(cast(buyer_bid as double)) as min_buyer_bid,
        | max(cast(buyer_bid as double)) as max_buyer_bid,
        | count(buyer_bid) as bin_count
        |FROM (
        | SELECT
        |  buyer_bid,
        |  row_number() OVER (ORDER BY cast(buyer_bid as double))  AS rank_id, --按照从小到大排序，给出一个序列id值
        |  count(1) over()  all_count
        |FROM jxbidview1)
        |GROUP BY CEIL(rank_id/(all_count/5))
        |""".stripMargin)

    println("########################################")
    bid_block.show(100)


    /**
     * 按照bid从小到大排序，之后按照区间分段，统计各个区间段内的的count
     * 等距分段，每个bin的bid区间是固定的，统计每个区间的频次。
     */
    val bid_block2 = spark.sql(
      """
        |  select
        |  count(case when cast(buyer_bid as double) >= 0.0 and cast(buyer_bid as double) < 20.0 then buyer_bid else null end) as count0_20,
        |  count(case when cast(buyer_bid as double) >= 20.0 and cast(buyer_bid as double) < 40.0 then buyer_bid else null end) as count20_40,
        |  count(case when cast(buyer_bid as double) >= 40.0 and cast(buyer_bid as double) < 60.0 then buyer_bid else null end) as count40_60,
        |  count(case when cast(buyer_bid as double) >= 60.0 and cast(buyer_bid as double) < 80.0 then buyer_bid else null end) as count60_80,
        |  count(case when cast(buyer_bid as double) >= 80.0 and cast(buyer_bid as double) < 100.0 then buyer_bid else null end) as count80_100,
        |  count(case when cast(buyer_bid as double) >= 100.0 and cast(buyer_bid as double) < 120.0 then buyer_bid else null end) as count100_120,
        |  count(case when cast(buyer_bid as double) >= 120.0 and cast(buyer_bid as double) < 140.0 then buyer_bid else null end) as count120_140,
        |  count(case when cast(buyer_bid as double) >= 140.0 and cast(buyer_bid as double) < 160.0 then buyer_bid else null end) as count140_160,
        |  count(case when cast(buyer_bid as double) >= 160.0 and cast(buyer_bid as double) < 180.0 then buyer_bid else null end) as count160_180,
        |  count(case when cast(buyer_bid as double) >= 180.0 and cast(buyer_bid as double) < 200.0 then buyer_bid else null end) as count180_200,
        |  count(case when cast(buyer_bid as double) >= 200.0 then buyer_bid else null end) as count200max
        |  from jxbidview1
        |""".stripMargin)

    println("########################################")
    bid_block2.show(100)


    /**
     * 按照bid从小到大排序，之后按照区间分段，统计各个区间段内的的count
     * 等距分段，每个bin的bid区间是固定的，统计每个区间的频次。
     */
    val bid_block_ldt = spark.sql(
      """
        |  select
        |  count(case when cast(buyer_bid as double) >= 0.0 and cast(buyer_bid as double) < 1500.0 then buyer_bid else null end) as count0_1500,
        |  count(case when cast(buyer_bid as double) >= 1500.0 and cast(buyer_bid as double) < 3000.0 then buyer_bid else null end) as count1500_3000,
        |  count(case when cast(buyer_bid as double) >= 3000.0 and cast(buyer_bid as double) < 4500.0 then buyer_bid else null end) as count3000_4500,
        |  count(case when cast(buyer_bid as double) >= 4500.0 and cast(buyer_bid as double) < 6000.0 then buyer_bid else null end) as count4500_6000,
        |  count(case when cast(buyer_bid as double) >= 6000.0 and cast(buyer_bid as double) < 7500.0 then buyer_bid else null end) as count6000_7500,
        |  count(case when cast(buyer_bid as double) >= 7500.0 and cast(buyer_bid as double) < 9000.0 then buyer_bid else null end) as count7500_9000,
        |  count(case when cast(buyer_bid as double) >= 9000.0 and cast(buyer_bid as double) < 10500.0 then buyer_bid else null end) as count9000_10500,
        |  count(case when cast(buyer_bid as double) >= 10500.0 and cast(buyer_bid as double) < 12000.0 then buyer_bid else null end) as count10500_12000,
        |  count(case when cast(buyer_bid as double) >= 12000.0 and cast(buyer_bid as double) < 13500.0 then buyer_bid else null end) as count12000_13500,
        |  count(case when cast(buyer_bid as double) >= 13500.0 and cast(buyer_bid as double) < 15000.0 then buyer_bid else null end) as count13500_15000,
        |  count(case when cast(buyer_bid as double) >= 15000.0 and cast(buyer_bid as double) < 16500.0 then buyer_bid else null end) as count15000_16500,
        |  count(case when cast(buyer_bid as double) >= 16500.0 and cast(buyer_bid as double) < 18000.0 then buyer_bid else null end) as count16500_18000,
        |  count(case when cast(buyer_bid as double) >= 18000.0 and cast(buyer_bid as double) < 19500.0 then buyer_bid else null end) as count18000_19500,
        |  count(case when cast(buyer_bid as double) >= 19500.0 and cast(buyer_bid as double) < 21000.0 then buyer_bid else null end) as count19500_21000,
        |  count(case when cast(buyer_bid as double) >= 21000.0 and cast(buyer_bid as double) < 22500.0 then buyer_bid else null end) as count21000_22500,
        |  count(case when cast(buyer_bid as double) >= 22500.0 and cast(buyer_bid as double) < 24000.0 then buyer_bid else null end) as count22500_24000,
        |  count(case when cast(buyer_bid as double) >= 24000.0 and cast(buyer_bid as double) < 25500.0 then buyer_bid else null end) as count24000_25500,
        |  count(case when cast(buyer_bid as double) >= 25500.0 and cast(buyer_bid as double) < 27000.0 then buyer_bid else null end) as count25500_27000,
        |  count(case when cast(buyer_bid as double) >= 27000.0 and cast(buyer_bid as double) < 28500.0 then buyer_bid else null end) as count27000_28500,
        |  count(case when cast(buyer_bid as double) >= 28500.0 and cast(buyer_bid as double) < 30000.0 then buyer_bid else null end) as count28500_30000,
        |  count(case when cast(buyer_bid as double) >= 30000.0 then buyer_bid else null end) as count30000max
        |  from jxbidview1
        |""".stripMargin)

    println("########################################")
    bid_block_ldt.show(100)



    /**
     * 中位数
     * 定义：对于有限的数集，可以通过把所有观察值高低排序后找出正中间的一个作为中位数。如果观察值有偶数个，通常取最中间的两个数值的平均数作为中位数
     * 这个难度比求众数大了一些，原因：
     * 求众数，并不要在意是否有脏数据（因为如果众数为脏数据，那么这个字段基本上可以删掉了），但是中位数就不一样，很有可能出现脏数据，因此，再求中位数的时候，得剔除掉脏数据（包括：空值、异常值等等）
     * 中位数先得求出总数，这个倒不难，难点在：比如有1万多条数据，就算你知道中位数就是第五千条，你怎么能把它找出来
     * 至于剔除脏数据，这里就不再演示了，直接看计算中位数的过程：已经封装为方法，直接用即可
     */

//    println("偶数中位数")
//    println(calc_middle(spark, dfzhongshuOS, "col1"))
//
//    println("奇数中位数")
//    println(calc_middle(spark, dfzhongshuJS, "col1"))

    val buyer_bid_desc = spark.sql(
      """
        |select cast(buyer_bid as double) as buyer_bid,rank_bid,
        |row_number() OVER (ORDER BY cast(buyer_bid as double))  AS rank_id
        |from jxbidview1
        |""".stripMargin)
    buyer_bid_desc.show()

    println("buyer_bid 中位数")
    println(calc_middle(spark, buyer_bid_desc, "buyer_bid"))

    sc.stop()
  }
}
