package pdp_project.pdp_project_classification

import org.apache.spark.sql._
import scala.collection.mutable.ListBuffer
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.sql._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.tree.configuration.Strategy
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics
object RandomForest2 {
	def main(args: Array[String]):Unit = {
			val conf = new SparkConf()
					.setAppName("Classify")
					.setMaster("yarn") // change this to yarn for aws
					val sc = new SparkContext(conf)
					val fractions = Map(0.0 -> 0.3895, 1.0 -> 0.0105)
					val trainingData = sc.textFile(args(0)).flatMap(line =>{
					val data = new ListBuffer[Tuple2[Double,Array[String]]]()
					val parts = line.split(',')
					val label = parts.last.toDouble
					val feature = parts.dropRight(1)
					
					data += new Tuple2(label, feature)
					val rotated90Feature = rotateMatrixBy90(parts.dropRight(1))
					//println(rotated90Feature.size)
					data += new Tuple2(label,rotated90Feature)
					val rotated270Feature = rotateMatrixBy270(parts.dropRight(1))
					//println(rotated270Feature.size)
					data += new Tuple2(label, rotated270Feature)
					val rotated180Feature = rotateMatrixBy180(parts.dropRight(1))
					//println(rotated180Feature.mport org.apache.spark.ml.Pipeline
					data += new Tuple2(label,rotated180Feature)
			data.map(x=>x)
			}).sampleByKey(false, fractions).map(x =>{
			  val  label = x._1
			  val features = x._2
			  LabeledPoint(label, Vectors.dense(features.map(_.toDouble)))
			})
		
//			println(trainingData.getNumPartitions)
//      val l = trainingData.glom().map(_.length).foreach(println) // get rlength of each partition
//			
			val numClasses = 2
      val categoricalFeaturesInfo = Map[Int, Int]()
      val numTrees = 30 // Use more in practice.
      val featureSubsetStrategy = "auto" // Let the algorithm choose.
      val impurity = "gini"
      val maxDepth = 5
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
	    val matrix  = rotate180Helper(t)
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