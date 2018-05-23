package pdp_project.pdp_project_classification

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
object RandomForest1 {
	def main(args: Array[String]):Unit = {
			val conf = new SparkConf()
					.setAppName("Classify")
					.setMaster("yarn") // change this to yarn for aws
					val sc = new SparkContext(conf)
					val trainingData = sc.textFile(args(0)).map { line =>
					val parts = line.split(',')
					val label = parts.last.toDouble
					val feature = parts.dropRight(1)
					LabeledPoint(label, Vectors.dense(feature.map(_.toDouble)))
			    }
			
			val numClasses = 2
      val categoricalFeaturesInfo = Map[Int, Int]()
      val numTrees = 200 // Use more in practice.
      val featureSubsetStrategy = "auto" // Let the algorithm choose.
      val impurity = "gini"
      val maxDepth = 10
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
}