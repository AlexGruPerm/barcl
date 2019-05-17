-- example: https://github.com/openscoring/papis.io/blob/master/ElasticNetAudit.scala
spark-shell --driver-memory 8G --executor-memory 8G  --executor-cores 1 --jars "/opt/spark-2.3.2/jars/spark-cassandra-connector-assembly-2.3.2.jar"  --jars "/opt/spark-2.3.2/jars/jpmml-sparkml-executable-1.4-SNAPSHOT.jar"    --conf spark.driver.maxResultSize=8G --conf "spark.cassandra.connection.host=192.168.122.192"

import org.apache.spark.sql._
import org.apache.spark.sql.functions.round
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import org.apache.spark.sql.types.{DoubleType}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.{DenseVector,SparseVector,Vectors}
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import spark.implicits._

import org.apache.spark.ml.feature.{StringIndexer,SQLTransformer}
import org.apache.spark.ml.{Pipeline, PipelineStage}
import scala.collection.mutable.ListBuffer
import org.jpmml.sparkml.PMMLBuilder

case class Form(res_type      :String,
                frmconfpeak   :String,
                sps           :Double,
                sms           :Double,
                tcvolprofile  :Int,
                acf_05_bws    :Double,
                acf_1_bws     :Double,
                acf_2_bws     :Double)

def showLogs(ds :Dataset[Form]) = println(" SIZE = "+ds.count)


 def getFormsDb(TickerID :Int, BarWidthSec: Int) :Dataset[Form] = {
            import org.apache.spark.sql.functions._
            spark.read.format("org.apache.spark.sql.cassandra")
              .options(Map("table" -> "bars_forms", "keyspace" -> "mts_bars"))
              .load()
              .where(col("ticker_id") === TickerID)
              .where(col("bar_width_sec") === BarWidthSec)
              .select(
                col("res_type"),
                col("formprops")("frmconfpeak").as("frmconfpeak"),
                col("formprops")("sps").as("sps").cast("Double"),
                col("formprops")("sms").as("sms").cast("Double"),
                col("formprops")("tcvolprofile").as("tcvolprofile").cast("Int"),
                col("formprops")("acf_05_bws").as("acf_05_bws").cast("Double"),
                col("formprops")("acf_1_bws").as("acf_1_bws").cast("Double"),
                col("formprops")("acf_2_bws").as("acf_2_bws").cast("Double")
                ).as[Form]}

------------------------------------------------------------------------------------------------------

val ds :Dataset[Form] = (1.to(4).by(1))/*Seq(6,8,10,14,15)*/.map(elm => getFormsDb(elm,30)).reduce(_ union _)
ds.cache()
showLogs(ds)


val stages = new ListBuffer[PipelineStage]()
stages += new StringIndexer().setInputCol("frmconfpeak").setOutputCol("confpeakIndex")
stages += new StringIndexer().setInputCol("res_type").setOutputCol("label")
stages += new VectorAssembler().setInputCols(Array("acf_1_bws","acf_2_bws","confpeakIndex")).setOutputCol("features")
stages += new SQLTransformer().setStatement("SELECT label, features FROM __THIS__")

val MLPCclassif = new MultilayerPerceptronClassifier().setLayers(Array[Int](3, 9, 5, 2)).setLabelCol("label").setFeaturesCol("features").setBlockSize(128).setSeed(1234L).setMaxIter(10)
stages += MLPCclassif

val splits = ds.randomSplit(Array(0.7, 0.3), seed = 1234L)
val train = splits(0)
val test = splits(1)

val estimator = new Pipeline().setStages(stages.toArray)
val model = estimator.fit(train)
val mlpc_predictions = model.transform(test)

val predictionAndLabels = mlpc_predictions.select("prediction", "label")
val evaluator = new MulticlassClassificationEvaluator().setMetricName("accuracy")
val accur = evaluator.evaluate(predictionAndLabels)


"acf_1_bws","acf_2_bws","confpeakIndex" (1-3) - 0.59
"acf_1_bws","acf_2_bws","confpeakIndex" (1-5) - 0.58
"acf_1_bws","acf_2_bws","confpeakIndex" (1-7) - 0.51 -- becouse USD correlated and anti pairs! :(
"acf_1_bws","acf_2_bws","confpeakIndex" (1-4)(30)  - 0.57  26556 rows.
"acf_1_bws","acf_2_bws","confpeakIndex" (1-4)(300) - 0.51
"acf_1_bws","acf_2_bws","confpeakIndex" (6,8,10,14,15 X/CAD)(30)  - 0.481
"acf_1_bws","acf_2_bws","confpeakIndex" (6,8,10,14,15 X/CAD)(300) -







println("Test set accuracy = "+accur)
/*

--Using model For 1 row.
val testRow = test.sample(false, 0.05).limit(1)
testRow.show()
model.transform(testRow).select("prediction", "label").show()

--Check model on any source data.
def checkModel(tickerIdx :Int, bws:Int) :Unit = {
 val dsTest :Dataset[Form] = getFormsDb(tickerIdx,bws)
 dsTest.cache()
 showLogs(dsTest)
 val mlpc_predictions = model.transform(dsTest)
 val predictionAndLabels = mlpc_predictions.select("prediction", "label")
 val evaluator = new MulticlassClassificationEvaluator().setMetricName("accuracy")
 val accur = evaluator.evaluate(predictionAndLabels)
 println("Test set accuracy for ["+tickerIdx+" - "+bws+"] = "+accur)
}

(1.to(38).by(1)).toList.map(tickerID => checkModel(tickerID,3600))

*/
