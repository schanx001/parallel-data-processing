package pdp_project.pdp_project_classification

import org.apache.spark.sql._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import scala.collection.mutable.ListBuffer
import scala.annotation.migration

object RandomForest4 {
	def main(args: Array[String]):Unit = {
			val conf = new SparkConf()
					.setAppName("Classify")
					.setMaster("local") // change this to yarn for aws
					val sc = new SparkContext(conf)

					val trainingData = sc.textFile(args(0)).flatMap(line =>{
					val data = new ListBuffer[LabeledPoint]()
					val parts = line.split(',')
					val label = parts.last.toDouble
					val feature = parts.dropRight(1)
					data += LabeledPoint(label, Vectors.dense(feature.map(_.toDouble)))
					val rotated90Feature = rotateMatrixBy90(parts.dropRight(1))
					data += LabeledPoint(label, Vectors.dense(rotated90Feature.map(_.toDouble)))
					val rotated270Feature = rotateMatrixBy270(parts.dropRight(1))
					data += LabeledPoint(label, Vectors.dense(rotated270Feature.map(_.toDouble)))
					val rotated180Feature = rotateMatrixBy180(parts.dropRight(1))
					data += LabeledPoint(label, Vectors.dense(rotated180Feature.map(_.toDouble)))
			data.map(x=>x)
			})
			val numClasses = 2
      val categoricalFeaturesInfo = Map[Int, Int]()
      val numTrees = 30 // Use more in practice.
      val featureSubsetStrategy = "auto" // Let the algorithm choose.
      val impurity = "gini"
      val maxDepth = 30
      val maxBins = 32
      val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
                  numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

      // Evaluate model on test instances and compute test error
      
			
      val validationData = sc.textFile(args(1)).map(line =>{

					val parts = line.split(',')
					val label = parts.last.toDouble
					val feature = parts.dropRight(1)

					var prediction  = model.predict(Vectors.dense(feature.map(_.toDouble))) 
					(label,prediction)
			})
			val acc = validationData.filter(r => r._1 == r._2).count.toDouble / validationData.count()
			println("accuracy : "+acc)
      
      // Save and load model
      model.save(sc,args(3)+"models")
      val sameModel = RandomForestModel.load(sc, args(3)+"models")
      
      var prediction1 = 0
      val testData = sc.textFile(args(2)).map { line =>

					val parts = line.split(',')
					val feature = parts.dropRight(1)
					var prediction  = sameModel.predict(Vectors.dense(feature.map(_.toDouble))) 
					prediction
			}.saveAsTextFile(args(3))
			
	}
	def rotateMatrixBy180(input:Array[String]):Array[String]={
	    val parent = split(input.toList,441) 
	  var output = Array("0")
	  for (i <- 0 to parent.size-1){
	    //println(parent(i).size)
	    val t = split(parent(i).toList,21)
	    //println(t.size)
	    //println(t(0).size)
	    val matrix  = rotate180Helper(t)
	    //matrix.transpose.map(_.reverse)
	    output ++= matrix.flatMap{case a: Array[String] => a}
	   
	  }
	  return output.drop(1)
	}
	
	
	def rotateMatrixBy90(input:Array[String]):Array[String]={
	  val parent = split(input.toList,441) 
	  var output = Array("0")
	  
	  for (i <- 0 to parent.size-1){
	    //println(parent(i).size)
	    val matrix = split(parent(i).toList,21)
	    //println(matrix.size)
	    //println(matrix(0).size)
	    matrix.transpose.map(_.reverse)
	    //println(matrix.flatten.size)
	    output ++= matrix.flatten
	    //println(output.last)
	  }
	  return output.drop(1)
	  
	}
	def split[A](xs: List[A], n: Int): List[List[A]] ={
    if (xs.isEmpty) Nil 
    else {
      val (ys, zs) = xs.splitAt(n)   
      ys :: split(zs, n)
    }
	}
	def rotateMatrixBy270(input:Array[String]):Array[String] = {
	  val parent = split(input.toList,441) 
	  var output = Array("0")
	  for (i <- 0 to parent.size-1){
	    //println(parent(i).size)
	    val t = split(parent(i).toList,21)
	    //println(t.size)
	    //println(t(0).size)
	    val matrix  = rotate270Helper(t)
	    //matrix.transpose.map(_.reverse)
	    output ++= matrix.flatMap{case a: Array[String] => a}
	   
	  }
	  return output.drop(1)

  }
	def rotate270Helper(matrix:List[List[String]]):Array[Array[String]] = {
	  val t = Array.ofDim[String](21, 21)
    for (i <- 0 to matrix.size-1){
      for (j <- 0 to matrix(0).size-1){
        t(j)(21-i-1) = matrix(i)(j)
      }
    }
	  t
	}
	def rotate180Helper(matrix:List[List[String]]):Array[Array[String]] = {
	  val t = Array.ofDim[String](21, 21)
    for (i <- 0 to matrix.size-1){
      for (j <- 0 to matrix(0).size-1){
        t(21-i-1)(21-j-1) = matrix(i)(j)
      }
    }
	  t
	}
}
