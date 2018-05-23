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
object Prediciting {
  
  def main(args: Array[String]):Unit = {
    val conf = new SparkConf()
					.setAppName("Classify")
					.setMaster("yarn") // change this to yarn for aws
					val sc = new SparkContext(conf)
    val sameModel = RandomForestModel.load(sc, args(1)+"models")
      var prediction1 = 0
      val testData = sc.textFile(args(0),1).map { line =>
					val parts = line.split(',')
					val feature = parts.dropRight(1)
					var prediction  = sameModel.predict(Vectors.dense(feature.map(_.toDouble))) 
					prediction
			}.saveAsTextFile(args(1))
  }
}