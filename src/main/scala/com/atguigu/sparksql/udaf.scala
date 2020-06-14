package com.atguigu.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * @BelongsProject: spark02
  * @BelongsPackage: com.atguigu.sparksql
  * @Author: liangyu
  * @CreateTime: 2019-12-07 15:05
  * @Description: ${Description}
  */
object udaf {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("udaf")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._
    val avgAge: AvgUdaf = new AvgUdaf()
    spark.udf.register("avgAge",avgAge)
    val df: DataFrame = spark.read.json("E:\\demo\\demo\\test.json")
    //    df.show()
    df.createOrReplaceTempView("User")
    spark.sql("select avgAge(age) from User").show()
    spark.close()
  }

}

class AvgUdaf extends UserDefinedAggregateFunction{
  override def inputSchema: StructType = {
    StructType(Array(StructField("age",IntegerType)))
  }

  override def bufferSchema: StructType = StructType(Array(StructField("sum",IntegerType),StructField("count",IntegerType)))

  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0)=0
    buffer(1)=0
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0)=buffer.getInt(0) + input.getInt(0)
    buffer(1)=buffer.getInt(1) + 1
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0)=buffer1.getInt(0) + buffer2.getInt(0)
    buffer1(1)=buffer1.getInt(1) + buffer2.getInt(1)
  }

  override def evaluate(buffer: Row): Any = {
    buffer.getInt(0).toDouble/buffer.getInt(1)
  }
}
