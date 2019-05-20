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

-- OK


val ds :Dataset[Form] = Seq(1,3,5).map(elm => getFormsDb(elm,30)).reduce(_ union _)
ds.cache()
showLogs(ds)

val stages = new ListBuffer[PipelineStage]()
stages += new StringIndexer().setInputCol("frmconfpeak").setOutputCol("confpeakIndex")
stages += new StringIndexer().setInputCol("tcvolprofile").setOutputCol("tcvolprofileIndex")
-------
stages += new StringIndexer().setInputCol("res_type").setOutputCol("label")
stages += new VectorAssembler().setInputCols(Array("tcvolprofileIndex","sps","acf_1_bws","acf_2_bws","confpeakIndex")).setOutputCol("features")
stages += new SQLTransformer().setStatement("SELECT label, features FROM __THIS__")

val MLPCclassif = new MultilayerPerceptronClassifier().setLayers(Array[Int](5, 9, 5, 2)).setLabelCol("label").setFeaturesCol("features").setBlockSize(128).setSeed(1234L).setMaxIter(10)
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
val model = estimator.fit(ds)


--====================================================================================================================================

correlation > 0 ~ 1
Seq(1,3,5)(30) - 0.8

correlation < 0

--====================================================================================================================================
-- research



val ds :Dataset[Form] = Seq(1,7).map(elm => getFormsDb(elm,30)).reduce(_ union _)
ds.cache()
showLogs(ds)

val stages = new ListBuffer[PipelineStage]()
-------
stages += new StringIndexer().setInputCol("res_type").setOutputCol("label")
stages += new VectorAssembler().setInputCols(Array("acf_05_bws","sms","sps")).setOutputCol("features")
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



--====================================================================================================================================




"acf_1_bws","acf_2_bws","confpeakIndex" (1-3) - 0.59
"acf_1_bws","acf_2_bws","confpeakIndex" (1-5) - 0.58
"acf_1_bws","acf_2_bws","confpeakIndex" (1-7) - 0.51 -- becouse USD correlated and anti pairs! :(
"acf_1_bws","acf_2_bws","confpeakIndex" (1-4)(30)  - 0.57  26556 rows.
"acf_1_bws","acf_2_bws","confpeakIndex" (1-4)(300) - 0.51

"acf_1_bws","acf_2_bws","confpeakIndex" 1.to(4) - (30) - 0.573
"acf_1_bws","acf_2_bws","confpeakIndex" 1.to(3) - (30) - 0.590
"acf_1_bws","acf_2_bws","confpeakIndex" (2,4)   - (30) - 0.55
"acf_1_bws","acf_2_bws","confpeakIndex" (1,3)   - (30) - 0.78   + 28 %
"acf_1_bws","acf_2_bws","confpeakIndex" (1,3,5) - (30) - 0.80 --- !!!!!!

"acf_1_bws","acf_2_bws","confpeakIndex" (1,3) - (300) - 0.52
"acf_1_bws","acf_2_bws","confpeakIndex" (1,3) - (600) - 0.53

model by ""acf_1_bws","acf_2_bws","confpeakIndex" (1,3) - (30)" on full data,

val model = estimator.fit(ds)

test model is (2) -
test model is (4) -

val mlpc_predictions = model.transform(test)
val predictionAndLabels = mlpc_predictions.select("prediction", "label")
val evaluator = new MulticlassClassificationEvaluator().setMetricName("accuracy")
val accur = evaluator.evaluate(predictionAndLabels)

(1.to(38).by(1)).foreach {elm =>
val dst :Dataset[Form] = getFormsDb(elm,30)
dst.cache()
showLogs(dst)
val mlpc_predictions = model.transform(dst)
val predictionAndLabels = mlpc_predictions.select("prediction", "label")
val evaluator = new MulticlassClassificationEvaluator().setMetricName("accuracy")
val accur = evaluator.evaluate(predictionAndLabels)
println(" TICKER = "+elm+ " accur = "+accur)
}

/*(1.to(3).by(1))*/


-- 1,3
 SIZE = 1183
 TICKER = 1 accur = 0.7962806424344886
 SIZE = 11055
 TICKER = 2 accur = 0.5622795115332428
 SIZE = 445
 TICKER = 3 accur = 0.7460674157303371
 SIZE = 13873
 TICKER = 4 accur = 0.5577740935630361
 SIZE = 511
 TICKER = 5 accur = 0.8493150684931506
 SIZE = 9407
 TICKER = 6 accur = 0.5020729244179866
 SIZE = 1297
 TICKER = 7 accur = 0.24441017733230533
 SIZE = 623
 TICKER = 8 accur = 0.5585874799357945
 SIZE = 517
 TICKER = 9 accur = 0.4796905222437137
 SIZE = 643
 TICKER = 10 accur = 0.578538102643857
 SIZE = 1311
 TICKER = 11 accur = 0.4851258581235698
 SIZE = 11902
 TICKER = 12 accur = 0.49050579734498406
 SIZE = 721
 TICKER = 13 accur = 0.5644937586685159
 SIZE = 10621
 TICKER = 14 accur = 0.5518312776574711
 SIZE = 9149
 TICKER = 15 accur = 0.5178708055525194
 SIZE = 9537
 TICKER = 16 accur = 0.5376952920205516
 SIZE = 1569
 TICKER = 17 accur = 0.5022307202039515
 SIZE = 9460
 TICKER = 18 accur = 0.5520084566596194
 SIZE = 748
 TICKER = 19 accur = 0.5989304812834224
 SIZE = 7012
 TICKER = 20 accur = 0.419281232173417
 SIZE = 210
 TICKER = 21 accur = 0.7095238095238096
 SIZE = 10799
 TICKER = 22 accur = 0.5342161311232522
 SIZE = 5625
 TICKER = 23 accur = 0.5345777777777778
 SIZE = 1534
 TICKER = 24 accur = 0.48826597131681876
 SIZE = 12587
 TICKER = 25 accur = 0.3626757765948995
 SIZE = 4904
 TICKER = 26 accur = 0.5065252854812398
 SIZE = 27498
 TICKER = 27 accur = 0.48308967924939994
 SIZE = 30293
 TICKER = 28 accur = 0.47783316277687915
 SIZE = 0
 TICKER = 29 accur = NaN
 SIZE = 4104
 TICKER = 30 accur = 0.4929337231968811
 SIZE = 5885
 TICKER = 31 accur = 0.4822429906542056
 SIZE = 10059
 TICKER = 32 accur = 0.5642708022666268
 SIZE = 14611
 TICKER = 33 accur = 0.5651221682294162
 SIZE = 8103
 TICKER = 34 accur = 0.4340367765025299
 SIZE = 2858
 TICKER = 35 accur = 0.5853743876836949
 SIZE = 6993
 TICKER = 36 accur = 0.47633347633347634
 SIZE = 13262
 TICKER = 37 accur = 0.5198310963655557
 SIZE = 2380
 TICKER = 38 accur = 0.5340336134453781




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


