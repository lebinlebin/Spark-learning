package org.training.spark.sql

/**
  * 数据集是货品交易数据集。
  * 每个订单可能包含多个货品，每个订单可以产生多次交易，不同的货品有不同的单价。
  * 1.计算所有订单中每年的销售单数、销售总额
  * 2. 统计每年最大金额订单的销售额
  * 3. 计算所有订单中每年最畅销货品
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

case class tbStock(ordernumber: String, locationid: String, dateid: String) extends Serializable

case class tbStockDetail(ordernumber: String, rownum: Int, itemid: String, number: Int, price: Double, amount: Double) extends Serializable

case class tbDate(dateid: String, years: Int, theyear: Int, month: Int, day: Int, weekday: Int, week: Int, quarter: Int, period: Int, halfmonth: Int) extends Serializable

object Practice {

  /**
    * 将DataFrame插入到Hive表
    */
  private def insertHive(spark: SparkSession, tableName: String, dataDF: DataFrame): Unit = {
    spark.sql("DROP TABLE IF EXISTS " + tableName)
    dataDF.write.saveAsTable(tableName)
  }

  private def insertMySQL(tableName: String, dataDF: DataFrame): Unit = {
    dataDF.write
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/sparksql")
      .option("dbtable", tableName)
      .option("user", "root")
      .option("password", "root")
      .mode(SaveMode.Overwrite)
      .save()
  }

  def main(args: Array[String]): Unit = {
    // 创建Spark配置
    val sparkConf = new SparkConf().setAppName("MockData").setMaster("local[*]")

    // 创建Spark SQL 客户端
    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    import spark.implicits._

    // 加载数据到Hive
    val tbStockRdd = spark.sparkContext.textFile("data/tbStock.txt")//时间维度
    val tbStockDS = tbStockRdd.map(_.split(",")).map(attr => tbStock(attr(0), attr(1), attr(2))).toDS
    insertHive(spark, "tbStock", tbStockDS.toDF)

    val tbStockDetailRdd = spark.sparkContext.textFile("data/tbStockDetail.txt")//订单详细信息
    val tbStockDetailDS = tbStockDetailRdd.map(_.split(",")).map(attr =>
      tbStockDetail(attr(0), attr(1).trim().toInt, attr(2), attr(3).trim().toInt, attr(4).trim().toDouble,
        attr(5).trim().toDouble)).toDS
    tbStockDetailDS.show()
//    insertHive(spark, "tbStockDetail", tbStockDetailDS.toDF)

    val tbDateRdd = spark.sparkContext.textFile("data/tbDate.txt")//
    val tbDateDS = tbDateRdd.map(_.split(",")).
        map(attr =>
          tbDate(attr(0), attr(1).trim().toInt, attr(2).trim().toInt, attr(3).trim().toInt,
            attr(4).trim().toInt, attr(5).trim().toInt, attr(6).trim().toInt, attr(7).trim().toInt,
            attr(8).trim().toInt, attr(9).trim().toInt)).toDS
    insertHive(spark, "tbDate", tbDateDS.toDF)

    //需求一： 统计所有订单中每年的销售单数、销售总额
    val result1 = spark.sql("SELECT c.theyear, COUNT(DISTINCT a.ordernumber), SUM(b.amount) " +
        "FROM tbStock a JOIN tbStockDetail b ON a.ordernumber = b.ordernumber JOIN tbDate c ON a.dateid = c.dateid " +
        "GROUP BY c.theyear ORDER BY c.theyear")
    result1.show()
//    insertMySQL("xq1",result1)

    //需求二： 统计每年最大金额订单的销售额
    val result2 = spark.sql("SELECT theyear, MAX(c.SumOfAmount) AS SumOfAmount " +
        "FROM (SELECT a.dateid, a.ordernumber, SUM(b.amount) AS SumOfAmount " +
        "FROM tbStock a JOIN tbStockDetail b ON a.ordernumber = b.ordernumber " +
        "GROUP BY a.dateid, a.ordernumber ) c JOIN tbDate d ON c.dateid = d.dateid " +
        "GROUP BY theyear ORDER BY theyear DESC")
    result2.show()
//    insertMySQL("xq2",result2)

    //需求三： 统计每年最畅销货品
    val result3 = spark.sql(
      "SELECT DISTINCT e.theyear, e.itemid, f.maxofamount " +
          "FROM (SELECT c.theyear, b.itemid, SUM(b.amount) AS sumofamount " +
          "FROM tbStock a JOIN tbStockDetail b ON a.ordernumber = b.ordernumber " +
          "JOIN tbDate c ON a.dateid = c.dateid GROUP BY c.theyear, b.itemid ) e " +
          "JOIN (SELECT d.theyear, MAX(d.sumofamount) AS maxofamount " +
          "FROM (SELECT c.theyear, b.itemid, SUM(b.amount) AS sumofamount " +
          "FROM tbStock a JOIN tbStockDetail b ON a.ordernumber = b.ordernumber " +
          "JOIN tbDate c ON a.dateid = c.dateid GROUP BY c.theyear, b.itemid ) d " +
          "GROUP BY d.theyear ) f ON e.theyear = f.theyear AND e.sumofamount = f.maxofamount ORDER BY e.theyear")
    result3.show()
//    insertMySQL("xq3",result3)

    spark.close
  }

}
