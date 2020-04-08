package pers.xmr.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
/**
  * @author xmr
  * @date 2019/4/29 10:32
  * @description @description spark转换操作RDD
  */
object SparkTransformRddScala {

  def resultCollect(rdd: RDD[Int]) = {
    val results = rdd.collect()
    for (i <- 0 to results.size - 1) {
      print(results(i) +" ")
    }
  }
  def resultCollectStr(rdd: RDD[String]) = {
    val results = rdd.collect()
    for (i <- 0 to results.size - 1) {
      print(results(i) +" ")
    }
  }

  def mapTest(rddScala: RDD[Int]): Unit = {
    val rdd = rddScala.map(i => i * 2)
    resultCollect(rdd)
  }

  def filter(rddScala: RDD[Int]): Unit = {
    val filter = rddScala.filter(i => i % 2 != 0)
    resultCollect(filter)
  }

  def flatMapTest(rddScala3: RDD[String]): Unit = {
    val flatMapRdd = rddScala3.flatMap(str => str.split(","))
    resultCollectStr(flatMapRdd)
  }

  def unionTest(scalaRDD1: RDD[Int], scalaRDD2: RDD[Int]): Unit = {
    val unionRDD = scalaRDD1.union(scalaRDD2)
    resultCollect(unionRDD)
  }

  def cartesionTest(scalaRDD1: RDD[Int], scalaRDD2: RDD[Int]): Unit = {
    val cartesianRdd = scalaRDD1.cartesian(scalaRDD2)
    val results = cartesianRdd.collect();
    for (result<- results) {
      print(result._1 + "," + result._2 + " ")
    }
  }

  def intersectionTest(scalaRDD1: RDD[Int], scalaRDD2: RDD[Int]): Unit = {
    val intersectionRDD = scalaRDD1.intersection(scalaRDD2)
    resultCollect(intersectionRDD)
  }

  def substractTest(scalaRDD1: RDD[Int], scalaRDD2: RDD[Int]): Unit = {
    val substractRDD = scalaRDD1.subtract(scalaRDD2)
    resultCollect(substractRDD)
  }

  def sampleTest(scalaRDD1: RDD[Int]): Unit = {
    val sampleRDD = scalaRDD1.sample(false, 0.1)
    resultCollect(sampleRDD)
  }

  def distinctTest(scalaRDD1: RDD[Int]) = {
    val distinctRDD = scalaRDD1.distinct()
    resultCollect(distinctRDD)
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("transform rdd test scala")
    val sparkContext = new SparkContext(sparkConf)
    val scalaRDD1 = sparkContext.parallelize(List(1, 2, 2, 3, 4, 5, 6, 7, 8, 9))
    val scalaRDD2 = sparkContext.parallelize(List(2, 4, 6, 8, 10))
    val scalaRDD3 = sparkContext.parallelize(List("1,3,5,7,9", "2,4,6,8,10"))
    mapTest(scalaRDD1)
    filter( scalaRDD1)
    flatMapTest(scalaRDD3)
    distinctTest(scalaRDD1)
    unionTest(scalaRDD1, scalaRDD2)
    cartesionTest(scalaRDD1, scalaRDD2)
    intersectionTest(scalaRDD1, scalaRDD2)
    substractTest(scalaRDD1, scalaRDD2)
    sampleTest(scalaRDD1)
  }
}
