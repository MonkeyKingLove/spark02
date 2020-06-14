package com.atguigu.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * @BelongsProject: spark02
  * @BelongsPackage: com.atguigu.spark
  * @Author: liangyu
  * @CreateTime: 2019-12-06 11:59
  * @Description: ${Description}
  */
object Spark53_Req_3_PageFlow {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark49_Req_1_Top10Category")
    val sc = new SparkContext(conf)
    //TODO:1.获取原始数据
    val datas: RDD[String] = sc.textFile("E:\\demo\\demo\\user_visit_action.txt")
    val userVisitActionRDD: RDD[UserVisitAction] = datas.map(
      line => {
        val ids = line.split("_")
        UserVisitAction(
          ids(0),
          ids(1).toLong,
          ids(2),
          ids(3).toLong,
          ids(4),
          ids(5),
          ids(6).toLong,
          ids(7).toLong,
          ids(8),
          ids(9),
          ids(10),
          ids(11),
          ids(12).toLong
        )
      }
    )

    //TODO:2.对原始数据进行过滤，得到想要获取的页面id的数据
    val pageList = List(1,2,3,4,5,6,7)
    val zipList: List[(Int, Int)] = pageList.zip(pageList.tail)
    val zip1: List[String] = zipList.map(
      action0 => action0._1 + "-" + action0._2
    )
    val pageRDD: RDD[(Long, Int)] = userVisitActionRDD.filter(
      action => {
        pageList.init.contains(action.page_id)
      }
    ).map(
      action1 => {
        (action1.page_id, 1)
      }
    )

    //TODO:3.对每个页面被访问的次数，分组聚合
    val pageToSumRDD: RDD[(Long, Int)] = pageRDD.reduceByKey(_+_)
    val fenMuSum: Map[Long, Int] = pageToSumRDD.collect().toMap
    val groupRDD: RDD[(String, Iterable[UserVisitAction])] = userVisitActionRDD.groupBy(
      action2 => {
        action2.session_id
      }
    )
    val map4RDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
      action3 => {
        val sortList: List[UserVisitAction] = action3.toList.sortWith(
          (left, right) => {
            left.action_time < right.action_time
          }
        )
        val mapRDD = sortList.map(
          action4 => {
            (action4.page_id, 1)
          }
        )
        val zip2RDD: List[((Long, Int), (Long, Int))] = mapRDD.zip(mapRDD.tail)
        val map3RDD: List[(String, Int)] = zip2RDD.map(action5 => (action5._1._1 + "-" + action5._2._1, 1)).filter(
          action8=>{
            zip1.contains(action8._1)
          }
        )
        map3RDD
      }
    )
    val map5RDD: RDD[(String, Int)] = map4RDD.flatMap(action6 =>action6._2)
    val pageIdToSumRDD: RDD[(String, Int)] = map5RDD.reduceByKey(_+_)
    pageIdToSumRDD.foreach(
      action7=>{
        val sum: Int = fenMuSum.getOrElse(action7._1.split("-")(0).toLong,1)
        println(action7._1 + ":::" + (action7._2*1.0/sum))
      }
    )
  }

}
