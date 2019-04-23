spark-shell --driver-memory 3G --executor-memory 5G  --executor-cores 1 --jars "/opt/spark-2.3.2/jars/spark-cassandra-connector-assembly-2.3.2.jar" --conf "spark.cassandra.connection.host=192.168.122.192"

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

val dsKeys = getKeysFormsDb;
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

-- manual research
spark.catalog.dropGlobalTempView("tmp")
ds.createGlobalTempView("tmp")

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
----------------------------------------------------