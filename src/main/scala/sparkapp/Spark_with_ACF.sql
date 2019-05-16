spark-shell --driver-memory 3G --executor-memory 5G  --executor-cores 1 --jars "/opt/spark-2.3.2/jars/spark-cassandra-connector-assembly-2.3.2.jar" --jars "/opt/spark-2.3.2/jars/" --conf "spark.cassandra.connection.host=192.168.122.192"

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

case class Form(ticker_id     :Int,
                bar_width_sec :Int,
                ts_begin      :Long,
                ts_end        :Long,
                log_oe        :Double,
                res_type      :String,
                formdeepkoef  :Int,
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

 def getFormsDb(TickerID :Int, BarWidthSec: Int) :Dataset[Form] = {
            import org.apache.spark.sql.functions._
            spark.read.format("org.apache.spark.sql.cassandra")
              .options(Map("table" -> "bars_forms", "keyspace" -> "mts_bars"))
              .load()
              .where(col("ticker_id") === TickerID)
              .where(col("bar_width_sec") === BarWidthSec)
              .select(
                col("ticker_id"),
                col("bar_width_sec"),
                col("ts_begin"),
                col("ts_end"),
                col("log_oe"),
                col("res_type"),
                col("formdeepkoef"),
                col("formprops")("frmconfpeak").as("frmconfpeak").cast("Int"),
                col("formprops")("sps").as("sps").cast("Double"),
                col("formprops")("sms").as("sms").cast("Double"),
                col("formprops")("tcvolprofile").as("tcvolprofile").cast("Int"),
                col("formprops")("acf_05_bws").as("acf_05_bws").cast("Double"),
                col("formprops")("acf_1_bws").as("acf_1_bws").cast("Double"),
                col("formprops")("acf_2_bws").as("acf_2_bws").cast("Double")
                ).sort(asc("ticker_id"),asc("bar_width_sec"),asc("ts_begin")).as[Form]}

val seqDsForms :Seq[Dataset[Form]] = dsKeys.collect.toSeq.map(elm => getFormsDb(elm.ticker_id,elm.bar_width_sec))
val ds :Dataset[Form] = seqDsForms.reduce(_ union _)
ds.cache()
ds.count()

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


--#1
-- Array[Int](9, 9, 5, 3)   setMaxIter(100)
Test set accuracy = 0.5191
Duration = 1800 - 1900

--RECREATE
spark.catalog.dropGlobalTempView("res")
result.createGlobalTempView("res")

total               -> 2412
label =  prediction -> 1252
label != prediction -> 1160

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