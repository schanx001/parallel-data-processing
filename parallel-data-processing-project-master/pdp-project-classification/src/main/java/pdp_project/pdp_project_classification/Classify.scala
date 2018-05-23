package pdp_project.pdp_project_classification

import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.SparkConf
import org.apache.spark;
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import scala.collection.mutable.ListBuffer
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.regression.LinearRegressionWithSGD

object Classify {
	def main(args: Array[String]):Unit = {
			val conf = new SparkConf()
					.setAppName("Classify")
					.setMaster("local") // change this to yarn for aws
					val sc = new SparkContext(conf)

					val parsedData = sc.textFile(args(0)+"TrainingData.csv").map { line =>

					val parts = line.split(',')
					val label = parts.last.toDouble
					val feature = parts.dropRight(1)
					LabeledPoint(label, Vectors.dense(feature.map(_.toDouble)))
			}.cache()
					// Building the model
					val numIterations = 100
					val stepSize = 0.00000001
					val model = LinearRegressionWithSGD.train(parsedData, numIterations, stepSize)

					// Evaluate model on training examples and compute training error
					val valuesAndPreds = parsedData.map { point =>
					val prediction = model.predict(point.features)
					(point.label, prediction)
			}
			val MSE = valuesAndPreds.map{ case(v, p) => math.pow((v - p), 2) }.mean()
					println(s"training Mean Squared Error $MSE")

					// Save and load model
					sc.parallelize(Seq(model),1).saveAsObjectFile("target/scalaLinearRegressionWithSGDModel")
					val sameModel = sc.objectFile[LinearRegressionModel]("target/scalaLinearRegressionWithSGDModel").first()
					//LinearRegressionModel.load(sc, "target/scalaLinearRegressionWithSGDModel")
					var labelP = 0
					val testData = sc.textFile(args(0)+"TestData1.csv").map { line =>

					val parts = line.split(',')
					val label = parts.last.toDouble
					val feature = parts.dropRight(1)

					if(model.predict(Vectors.dense(feature.map(_.toDouble))) > 0.5) 
						  labelP = 0 
					else 
							labelP = 1
							("original-label => " + label,"predicted label=> "+ label)
			}.saveAsTextFile("output1")

	}
}