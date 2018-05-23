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

object RandomForest3 {
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
					//val rotated90Feature = rotateMatrixBy90(parts.dropRight(1))
					//println(rotated90Feature.size)
					//data += LabeledPoint(label, Vectors.dense(rotated90Feature.map(_.toDouble)))
					//val rotated270Feature = rotateMatrixBy270(parts.dropRight(1))
					//println(rotated270Feature.size)
					//data += LabeledPoint(label, Vectors.dense(rotated270Feature.map(_.toDouble)))
					//val rotated180Feature = rotateMatrixBy180(parts.dropRight(1))
					//println(rotated180Feature.mport org.apache.spark.ml.Pipeline

					//data += LabeledPoint(label, Vectors.dense(rotated180Feature.map(_.toDouble)))
			data.map(x=>x)
			}).sample(false, 0.7, 1)
			println(trainingData.getNumPartitions)
      val l = trainingData.glom().map(_.length).foreach(println) // get rlength of each partition
			
      var bestModel : Option[RandomForestModel] = None
      var prevERR = Double.MaxValue
      val parameters = List( (10,30,45),(40,20,32),(15,15,43))
      for(line <- parameters) {
        
  			val numClasses = 2
        val categoricalFeaturesInfo = Map[Int, Int]()
        val numTrees = line._1 // Use more in practice.
        val featureSubsetStrategy = "auto" // Let the algorithm choose.
        val impurity = "gini"
        val maxDepth = line._2
        val maxBins = line._3
        val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
                    numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)
  
        // Evaluate model on test instances and compute test error
        val labelAndPreds = trainingData.map { point =>
          val prediction = model.predict(point.features)
          (point.label, prediction)
        }
        val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / trainingData.count()
        println(s"Test Error = $testErr")
        if (prevERR >= testErr){
          bestModel = Some(model)
          //println(bestModel.getOrElse("empty"))
          prevERR = testErr
        } 
			}
	
			println("Learned classification forest model:\n" + bestModel.get.toDebugString)
      // Save and load model
     // bestModel.get.save(sc,args(2)+"models")
      
//      val sameModel = RandomForestModel.load(sc, args(2)+"models")
//      
//      var prediction1 = 0
//      val testData = sc.textFile(args(1)).map { line =>
//
//					val parts = line.split(',')
//					val label = parts.last.toDouble
//					val feature = parts.dropRight(1)
//
//					var prediction  = sameModel.predict(Vectors.dense(feature.map(_.toDouble))) 
//					if (prediction==label){prediction1 = 1} else {prediction1 = 0}
//							("original-label => " + label,"predicted label=> "+ prediction+" Prediction was"+prediction1)
//			}.saveAsTextFile(args(2))
			
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