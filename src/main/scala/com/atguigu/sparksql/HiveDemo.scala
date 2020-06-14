package com.atguigu.sparksql

import org.apache.spark.sql.SparkSession

/**
  * @BelongsProject: spark02
  * @BelongsPackage: com.atguigu.sparksql
  * @Author: liangyu
  * @CreateTime: 2019-12-09 10:11
  * @Description: ${Description}
  */
object HiveDemo {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").
    enableHiveSupport().
//    config("spark.sql.warehouse.dir", "hdfs://hadoop202:9000/user/hive/warehouse").
    appName("HiveDemo").
    getOrCreate()

    import spark.implicits._
    import spark.sql
    sql("use spark_sql")
    sql(
      """
        |select
        |c.*,
        |p.product_name,
        |u.click_product_id
        |from user_visit_action u
        |join product_info p on u.click_product_id = p.product_id
        |join city_info  c on u.city_id = c.city_id
        |where u.click_product_id > -1
      """.stripMargin
    ).show()
  }

}
