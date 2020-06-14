package com.atguigu.sparksql

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @BelongsProject: spark02
  * @BelongsPackage: com.atguigu.sparksql
  * @Author: liangyu
  * @CreateTime: 2019-12-07 16:54
  * @Description: ${Description}
  */
object SparkSQL11_JDBC {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]")
      .appName("SparkSQL11_JDBC")
      .getOrCreate()
    val df: DataFrame = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://hadoop102:3306/gmall")
      .option("user", "root")
      .option("password", "atguigu")
      .option("dbtable", "base_category2")
      .load()
    df.show()
    spark.close()
  }
  //写一个udaf：计算平均个数

}
