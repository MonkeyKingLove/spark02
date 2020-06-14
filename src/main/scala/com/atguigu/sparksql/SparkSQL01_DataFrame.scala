package com.atguigu.sparksql

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @BelongsProject: spark02
  * @BelongsPackage: com.atguigu.sparksql
  * @Author: liangyu
  * @CreateTime: 2019-12-07 16:51
  * @Description: ${Description}
  */
object SparkSQL01_DataFrame {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().
      master("local[*]").appName("SparkSQL01_DataFrame")
      .getOrCreate()
    import spark.implicits._
    val df: DataFrame = spark.read.json("E:\\demo\\demo\\test.json")
    df.createOrReplaceTempView("user")
//    spark.sql("select * from user").show()
    df.select("age","name").show()


  }

}
