package pdp_project.pdp_project_classification
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
object GradientTrees {
  def main(args: Array[String]):Unit = {
    val conf = new SparkConf()
					.setAppName("Classify")
					.setMaster("local") // change this to yarn for aws
					val sc = new SparkContext(conf)
// Load and parse the data file.
    val trainingData = sc.textFile(args(0)).map { line =>
      val parts = line.split(',')
    	val label = parts.last.toDouble
    	val feature = parts.dropRight(1)
    	LabeledPoint(label, Vectors.dense(feature.map(_.toDouble)))
    }
    
    // Train a GradientBoostedTrees model.
    // The defaultParams for Classification use LogLoss by default.
    val model = new LogisticRegressionWithLBFGS()
      .setNumClasses(2)
      .run(trainingData)
    
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
      val sameModel = LogisticRegressionModel.load(sc, args(3)+"models")
      var prediction1 = 0
      val testData = sc.textFile(args(2)).map { line =>
					val parts = line.split(',')
					val feature = parts.dropRight(1)
					var prediction  = sameModel.predict(Vectors.dense(feature.map(_.toDouble))) 
					prediction
			}.saveAsTextFile(args(3))
    
    
  }
}