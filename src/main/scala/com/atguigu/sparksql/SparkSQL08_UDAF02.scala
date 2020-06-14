package com.atguigu.sparksql

import org.apache.spark.sql._
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator

/**
  * @BelongsProject: spark02
  * @BelongsPackage: com.atguigu.sparksql
  * @Author: liangyu
  * @CreateTime: 2019-12-07 16:53
  * @Description: ${Description}
  */
object SparkSQL08_UDAF02 {
  def main(args: Array[String]): Unit = {
    // 创建Spark环境配置对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL08_UDAF02")

    // 创建上下文环境对象
    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    // 创建自定义聚合函数
    val udaf = new AvgAgeUDAFClass()
    // 将聚合函数当成查询的列
    val column: TypedColumn[UserX, Double] = udaf.toColumn

    // 用户自定义聚合函数 ： UDAF
    val df: DataFrame = spark.read.json("E:\\demo\\demo\\test.json")

    val ds: Dataset[UserX] = df.as[UserX]

    // 采用DSL语法
    //column:包含输入和输出的类型
    ds.select(column).show()


    // 释放资源
    spark.close()
  }

}
case class UserX( age:Long,name:String)
case class AvgBuffer( var sum:Long, var count:Long )
class AvgAgeUDAFClass extends Aggregator[UserX,AvgBuffer,Double]{
  override def zero: AvgBuffer = {
    AvgBuffer(0L,0L)
  }

  override def reduce(b: AvgBuffer, a: UserX): AvgBuffer = {
    b.sum = b.sum + a.age
    b.count = b.count + 1L
    b
  }

  override def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = {
    b1.sum = b1.sum + b2.sum
    b1.count = b1.count + b2.count
    b1
  }

  override def finish(reduction: AvgBuffer): Double = {
    reduction.sum.toDouble/reduction.count
  }

  override def bufferEncoder: Encoder[AvgBuffer] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}
