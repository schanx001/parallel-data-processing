package com.scala.part2

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object SecondarySort {
  def main(args: Array[String]) = {
  val conf = new SparkConf()
      .setAppName("Average")
      .setMaster("local")
    val sc = new SparkContext(conf)
  
    //Read some example file to a test RDD
    // emitting key as (stationid, year) and value tmax and tmin records with temperature and count
    val input = sc.textFile("/home/schanx/parallel-data-processing/parallel-data-processing/Homework2/part2/input/*.csv")
    .map(line => line.split(",")).filter(fields => fields(2)=="TMAX" || fields(2)=="TMIN")
    .map(fields=> if(compare(fields(2))) ((fields(0),fields(1).substring(0,4)),(Integer.parseInt(fields(3)),1,0,0)) 
                  else ((fields(0),fields(1).substring(0,4)),(0,0,Integer.parseInt(fields(3)),1))).sortBy(_._1._2)
                  

    // reducing on key stationid and calculating the average 
    // after reducing , mapping on records and emitting required output
    // finally grouping by key to get the time series per station
    val avg = input.reduceByKey((a,b)=> 
        (a._1+b._1,
               a._2+b._2,
               a._3+b._3,
               a._4+b._4)).map((x)=> (x._1._1,(x._1._2,x._2._1.toDouble/x._2._2,x._2._3.toDouble/x._2._4))).sortBy(_._2._1)
               .groupByKey()
               .mapValues(_.toList).saveAsTextFile("secondarySortOutput.txt")
     }     
  def compare(x:String): Boolean = return (x=="TMAX")
}