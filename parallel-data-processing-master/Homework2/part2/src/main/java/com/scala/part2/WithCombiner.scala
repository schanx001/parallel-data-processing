package com.scala.part2
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object WithCombiner {
   def main(args: Array[String]) = {
    val conf = new SparkConf()
      .setAppName("Average")
      .setMaster("local")
    val sc = new SparkContext(conf)
    // Read some example file to a test RDD
    // emitting key as stationId 
    val input = sc.textFile("/home/schanx/parallel-data-processing/parallel-data-processing/Homework2/part2/input/*.csv")
    .map(line => line.split(",")).filter(fields => fields(2)=="TMAX" || fields(2)=="TMIN")
    .map(fields=> if(compare(fields(2))) (fields(0),(Integer.parseInt(fields(3)),1,0,0)) 
                  else (fields(0),(0,0,Integer.parseInt(fields(3)),1)))
    
    // combining output from mapper and accumulating the sum and count for records per station
    val comInput = input.combineByKey(v => {if (v._1 == 0) (v._1,0,v._3,1) else (v._1,1,v._3,0)},
                        (a: (Int,Int,Int,Int),v)=>{ if (v._1 == 0) (a._1+ 0, a._2 + 0, a._3 + v._3, a._4 + 1)
                        else (a._1+ v._1, a._2 + 1, a._3 + 0, a._4 + 0)},
                        (a:(Int,Int,Int,Int),b: (Int,Int,Int,Int)) => { (a._1 + b._1, a._2+b._2, a._3+b._3,a._4+b._4)})
      
     comInput.foreach(println) 
     val avg = comInput.map(x=>(x._1,x._2._1.toDouble/x._2._2,x._2._3.toDouble/x._2._4)).saveAsTextFile("outputWithCombiner.txt")
     
   }    
  def compare(x:String): Boolean = return (x=="TMAX")

}