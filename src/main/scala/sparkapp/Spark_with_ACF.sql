spark-shell --driver-memory 8G --executor-memory 8G  --executor-cores 1 --jars "/opt/spark-2.3.2/jars/spark-cassandra-connector-assembly-2.3.2.jar" --jars "/opt/spark-2.3.2/jars/"  --conf spark.driver.maxResultSize=8G --conf "spark.cassandra.connection.host=192.168.122.192"

import org.apache.spark.sql._
import org.apache.spark.sql.functions.round
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import org.apache.spark.sql.types.{DoubleType}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.linalg.{DenseVector,SparseVector}
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import spark.implicits._

case class FormKeys(ticker_id     :Int,
                    bar_width_sec :Int)

/*
               ticker_id     :Int,
                bar_width_sec :Int,
                ts_begin      :Long,
                ts_end        :Long,
                log_oe        :Double,
                res_type      :String,
                formdeepkoef  :Int,
*/

case class Form(
                res_type      :String,
                frmconfpeak   :Int,
                sps           :Double,
                sms           :Double,
                tcvolprofile  :Int,
                acf_05_bws    :Double,
                acf_1_bws     :Double,
                acf_2_bws     :Double
                )

case class ccLibSvm(label    :Int,
                    features :Int)

 def getKeysFormsDb :Dataset[FormKeys] = {
            import org.apache.spark.sql.functions._
            spark.read.format("org.apache.spark.sql.cassandra")
              .options(Map("table" -> "bars_forms", "keyspace" -> "mts_bars"))
              .load()
              .select(
                col("ticker_id"),
                col("bar_width_sec")
                ).distinct.sort(asc("ticker_id"),asc("bar_width_sec")).as[FormKeys]}

val dsKeys = getKeysFormsDb
dsKeys.cache()
dsKeys.show()


/*
              .select(
                col("ticker_id"),
                col("bar_width_sec"),
                col("ts_begin"),
                col("ts_end"),
                col("log_oe"),
                col("res_type"),
                col("formdeepkoef"),
                ...
                ).sort(asc("ts_begin"))
*/

 def getFormsDb(TickerID :Int, BarWidthSec: Int) :Dataset[Form] = {
            import org.apache.spark.sql.functions._
            spark.read.format("org.apache.spark.sql.cassandra")
              .options(Map("table" -> "bars_forms", "keyspace" -> "mts_bars"))
              .load()
              .where(col("ticker_id") === TickerID)
              .where(col("bar_width_sec") === BarWidthSec)
              .select(
                col("res_type"),
                col("formprops")("frmconfpeak").as("frmconfpeak").cast("Int"),
                col("formprops")("sps").as("sps").cast("Double"),
                col("formprops")("sms").as("sms").cast("Double"),
                col("formprops")("tcvolprofile").as("tcvolprofile").cast("Int"),
                col("formprops")("acf_05_bws").as("acf_05_bws").cast("Double"),
                col("formprops")("acf_1_bws").as("acf_1_bws").cast("Double"),
                col("formprops")("acf_2_bws").as("acf_2_bws").cast("Double")
                ).as[Form]}


val seqDsForms :Seq[Dataset[Form]] = dsKeys.collect.toSeq.map(elm => getFormsDb(elm.ticker_id,elm.bar_width_sec))
val ds :Dataset[Form] = seqDsForms.reduce(_ union _)
ds.cache()
ds.count()

res6: Long = 509037

/*
Job aborted due to stage failure:
Total size of serialized results of 7114 tasks (1024.0 MB)
is bigger than spark.driver.maxResultSize (1024.0 MB)
*/

val trans =
ds.withColumn("label",
   when($"res_type" === "mx", 1).otherwise(
    when($"res_type" === "mn", 2).otherwise(3))).
     select(
      col("label").cast(DoubleType),
      col("frmconfpeak"),
      col("sps"),
      col("sms"),
      col("tcvolprofile"),
      col("acf_05_bws"),
      col("acf_1_bws"),
      col("acf_2_bws")
     ).withColumn("f1", round(col("frmconfpeak")/100)).withColumn("f2", round((col("frmconfpeak") - round(col("frmconfpeak")/100)*100)/10)).withColumn("f3", col("frmconfpeak") % 10)


val mlsrc = ds.withColumn("label",
   when($"res_type" === "mx", 1).otherwise(
    when($"res_type" === "mn", 2).otherwise(3))).
     select(
      col("label").cast(DoubleType),
      col("frmconfpeak"),
      col("sps"),
      col("sms"),
      col("tcvolprofile"),
      col("acf_05_bws"),
      col("acf_1_bws"),
      col("acf_2_bws")
     ).withColumn("f1", round(col("frmconfpeak")/100)).withColumn("f2", round((col("frmconfpeak") - round(col("frmconfpeak")/100)*100)/10)).withColumn("f3", col("frmconfpeak") % 10)

val clr = mlsrc
val assembler = new VectorAssembler().setInputCols(Array("f1","f2","f3","sps","sms","tcvolprofile","acf_05_bws","acf_1_bws","acf_2_bws")).setOutputCol("features")
val dat = assembler.transform(clr)
val data = dat.drop("f1").drop("f2").drop("f3").drop("sps").drop("sms").drop("tcvolprofile").drop("acf_05_bws").drop("acf_1_bws").drop("acf_2_bws")
val splits = data.randomSplit(Array(0.7, 0.3), seed = 1234L)
val train = splits(0)
val test = splits(1)
println(" rows:"+data.count()+" train:"+train.count()+" test:"+test.count())

rows:509037 train:356419 test:152618

val t1 = System.currentTimeMillis
val layers = Array[Int](9, 9, 5, 3)
val trainer = new MultilayerPerceptronClassifier().setLayers(layers).setLabelCol("label").setFeaturesCol("features").setBlockSize(128).setSeed(1234L).setMaxIter(10)
val model = trainer.fit(train)
val result = model.transform(test)
val predictionAndLabels = result.select("prediction", "label")
val evaluator = new MulticlassClassificationEvaluator().setMetricName("accuracy")
println(s"Test set accuracy = ${evaluator.evaluate(predictionAndLabels)}")
val t2 = System.currentTimeMillis
println("Duration of FormsBuilder.run() - "+(t2 - t1)/1000 + " sec.")

scala> println(s"Test set accuracy = ${evaluator.evaluate(predictionAndLabels)}")
Test set accuracy = 0.51
Duration of FormsBuilder.run() - 510 sec.



--#####################################################################################################################

ds.show(5)

+--------+-----------+-----+-----+------------+----------+---------+---------+
|res_type|frmconfpeak|  sps|  sms|tcvolprofile|acf_05_bws|acf_1_bws|acf_2_bws|
+--------+-----------+-----+-----+------------+----------+---------+---------+
|      mx|        331|0.442|0.558|           4|     0.905|    0.788|    0.652|
|      mx|        133|0.405|0.595|           2|     0.782|    0.712|    0.578|
|      mx|        332|0.391|0.609|           4|    -0.072|   -0.145|   -0.861|
|      mx|        323|0.592|0.408|           4|     0.395|   -0.115|   -0.707|
|      mx|        131|0.508|0.492|           2|     0.187|   -0.044|   -0.172|
+--------+-----------+-----+-----+------------+----------+---------+---------+

import org.apache.spark.ml.feature.SQLTransformer
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.jpmml.sparkml.PMMLBuilder

---------------------------------------------------------------------------------------------------------------------
-- 1.
-- SQLTransformer is a Transformer that does transformations by executing SELECT …​ FROM THIS
--
---------------------------------------------------------------------------------------------------------------------
val sqlInitialTransformer = new SQLTransformer()

sqlInitialTransformer.setStatement("""
                       SELECT
                         CASE WHEN res_type = 'mx'
                         THEN 1
                         ELSE CASE WHEN res_type = 'mn'
                              THEN 2
                         ELSE 3 END
                        END as label,
                         frmconfpeak,
                         sps,
                         sms,
                         tcvolprofile,
                         acf_05_bws,
                         acf_1_bws,
                         acf_2_bws,
                         round(frmconfpeak/100) as f1,
                         round((frmconfpeak - round(frmconfpeak/100)*100)/10) as f2,
                         (frmconfpeak % 10) as f3
                        FROM __THIS__
                       """).transform(ds).show

val ds1 = sqlInitialTransformer.transform(ds)

---------------------------------------------------------------------------------------------------------------------
-- 2.
-- VectorAssembler is a feature transformer that assembles (merges) multiple columns into a (feature) vector column.
--
---------------------------------------------------------------------------------------------------------------------

val vecAssembler = new VectorAssembler()

val features = vecAssembler.setInputCols(Array("f1","f2","f3","sps","sms","tcvolprofile","acf_05_bws","acf_1_bws","acf_2_bws")).setOutputCol("features").transform(ds1)

---------------------------------------------------------------------------------------------------------------------
-- 3.
-- SQLTransformer is a Transformer that does transformations by executing SELECT …​ FROM THIS
--
---------------------------------------------------------------------------------------------------------------------
val sqlPostTransformer = new SQLTransformer()

val dsRes = sqlPostTransformer.setStatement("""SELECT label, features FROM __THIS__ """).transform(features)

dsRes.show(5)

---------------------------------------------------------------------------------------------------------------------
-- 4.
-- SQLTransformer is a Transformer that does transformations by executing SELECT …​ FROM THIS
--
---------------------------------------------------------------------------------------------------------------------
val classifier = new MultilayerPerceptronClassifier().setLayers(layers).setLabelCol("label").setFeaturesCol("features")
 .setBlockSize(128).setSeed(1234L).setMaxIter(10)

val pipeline = new Pipeline().setStages(Array(sqlInitialTransformer, vecAssembler, sqlPostTransformer, classifier))
val pipelineModel = pipeline.fit(ds)

val pmml = new PMMLBuilder(ds.schema, pipelineModel).build()
pmmlBuilder.buildFile(new File("/root/pmml/MLPPBarcl.pmml"))



-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
-- ====================================================================================================================







import java.io.File
import java.util.NoSuchElementException

import scala.collection.mutable.ListBuffer


def prepareDataset(df: Dataset[Form]): DataFrame = {
df.withColumn("label",
   when($"res_type" === "mx", 1).otherwise(
    when($"res_type" === "mn", 2).otherwise(3))).
     select(
      col("label").cast(DoubleType),
      col("frmconfpeak"),
      col("sps"),
      col("sms"),
      col("tcvolprofile"),
      col("acf_05_bws"),
      col("acf_1_bws"),
      col("acf_2_bws")
     ).withColumn("f1", round(col("frmconfpeak")/100))
      .withColumn("f2", round((col("frmconfpeak") - round(col("frmconfpeak")/100)*100)/10))
      .withColumn("f3", col("frmconfpeak") % 10)
}

def assemblyFutures(df: DataFrame): DataFrame = {
(new VectorAssembler().setInputCols(Array("f1","f2","f3","sps","sms","tcvolprofile","acf_05_bws","acf_1_bws","acf_2_bws")).setOutputCol("features")).transform(df)
.drop("f1").drop("f2").drop("f3").drop("sps").drop("sms").drop("tcvolprofile").drop("acf_05_bws").drop("acf_1_bws").drop("acf_2_bws").drop("frmconfpeak")
}

val weirdDf = ds.transform(prepareDataset).transform(assemblyFutures)

weirdDf.show(5)

+-----+--------------------+
|label|            features|
+-----+--------------------+
|  1.0|[3.0,3.0,1.0,0.44...|
|  1.0|[1.0,3.0,3.0,0.40...|
|  1.0|[3.0,3.0,2.0,0.39...|
|  1.0|[3.0,2.0,3.0,0.59...|
|  1.0|[1.0,3.0,1.0,0.50...|
+-----+--------------------+

val weirdDfSchema = weirdDf.schema()

val classifier = new MultilayerPerceptronClassifier().setLayers(layers).setLabelCol("label").setFeaturesCol("features").setBlockSize(128).setSeed(1234L).setMaxIter(10)

val pipeline = new Pipeline().setStages(Array(classifier))

val pipelineModel = pipeline.fit(weirdDf)

val pmml = new PMMLBuilder(weirdDf.schema, pipelineModel).build()


pmmlBuilder.buildFile(new File("/root/pmml/MLPPBarcl.pmml"))

--###########################################################################################################################################################

import org.apache.spark.ml.{Pipeline, PipelineStage}

/**
*
* Pipeline example
*  val stages: Array[PipelineStage] = indexers ++ encoders
*/


val datprep = (new VectorAssembler().setInputCols(Array("f1","f2","f3","sps","sms","tcvolprofile","acf_05_bws","acf_1_bws","acf_2_bws")).setOutputCol("features")).transform(mlsrc).drop("f1").drop("f2").drop("f3").drop("sps").drop("sms").drop("tcvolprofile").drop("acf_05_bws").drop("acf_1_bws").drop("acf_2_bws")

val pipeline = new Pipeline().setStages(Array(trans,classifier))


val pipeline = new Pipeline().setStages(stages)
val startTime = System.nanoTime
pipeline.fit(df).transform(df).show
val runningTime = System.nanoTime — startTime





//final model to save into PMML XML file
val model = trainer.fit(data)


import org.jpmml.sparkml.ConverterUtil

val schema = data.schema()
PipelineModel pipelineModel = pipeline.fit(dataFrame);
org.dmg.pmml.PMML pmml = org.jpmml.sparkml.ConverterUtil.toPMML(schema, pipelineModel);
JAXBUtil.marshalPMML(pmml, new StreamResult(System.out));





//SAVE model for future using in onlinescoring REST API !!!

--#1
--RECREATE
spark.catalog.dropGlobalTempView("res")
result.createGlobalTempView("res")


spark.sql(""" select count(*)
                from global_temp.res
          """).show()


-----------------------------------------------------------------------------------------------------------------------
-- manual research
spark.catalog.dropGlobalTempView("tmp")
ds.createGlobalTempView("tmp")

--mx
spark.sql(""" select frmconfpeak,sum(1) as cnt
                from global_temp.tmp
               where sps>sms and
                     res_type='mx' and
                     acf_05_bws > 0.9 and
                     acf_05_bws > acf_1_bws and acf_1_bws >  acf_2_bws and
                     (acf_1_bws>0.9 and acf_2_bws>0.9)
             group by frmconfpeak
             order by 2 desc
          """).show()

--mn
spark.sql(""" select frmconfpeak,sum(1) as cnt
                from global_temp.tmp
               where sps>sms and
                     res_type='mn' and
                     acf_05_bws > 0.9 and
                     acf_05_bws > acf_1_bws and acf_1_bws >  acf_2_bws and
                     (acf_1_bws>0.9 and acf_2_bws>0.9)
             group by frmconfpeak
             order by 2 desc
          """).show()

spark.sql(""" select * from global_temp.tmp """).show()