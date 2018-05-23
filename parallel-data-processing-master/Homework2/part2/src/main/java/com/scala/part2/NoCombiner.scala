package com.scala.part2

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object NoCombiner {
  def main(args: Array[String]) = {
    val conf = new SparkConf()
      .setAppName("Average")
      .setMaster("local")
    val sc = new SparkContext(conf)
    
    //Read some example file to a test RDD
    // check if records are of type tmax or tmin
    val input = sc.textFile("/home/schanx/parallel-data-processing/parallel-data-processing/Homework2/part2/input/*.csv")
    .map(line => line.split(","))
    .filter(fields => fields(2)=="TMAX" || fields(2)=="TMIN")
    .map(fields=> 
      if(compare(fields(2))) (fields(0),(Integer.parseInt(fields(3)),1,0,0)) 
      else (fields(0),(0,0,Integer.parseInt(fields(3)),1)))
    
     
    // calculating sum and count per station  
    val sumByKey = input.reduceByKey((r1,r2)=> (r1._1+r2._1,r1._2+r2._2,r1._3+r2._3,r1._4+r2._4))
    
    // calculating average per station
    val avgByKey = sumByKey.map((x)=> 
       if(checkIfZero(x._2._2,x._2._4)==1) 
       (x._1,(0,x._2._3.toDouble/x._2._4)) 
       else if(checkIfZero(x._2._2,x._2._4)==2) (x._1,(x._2._1.toDouble/x._2._2,0))
       else (x._1,(x._2._1.toDouble/x._2._2,x._2._3.toDouble/x._2._4)))
       .saveAsTextFile("outputNoCombiner.txt")
       
    sc.stop()
  }
  
  // check if any record has 0 count which means that it might not have tmin or tmax record
  def checkIfZero(c1:Int,c2:Int): Int = {
    if(c1==0)
      return 1
    else if(c2==0)
     return 2
    else
      return 3
  }
  
  // checking if record is of type tmax else tmin
  def compare(x:String): Boolean = return (x=="TMAX")

}