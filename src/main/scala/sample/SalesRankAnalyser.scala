package sample

import com.twitter.scalding.Args
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object SalesRankAnalyser extends App{

  val cmdArgs = Args(args)
  val reviewsInputPath = cmdArgs("reviews")
  val metadataInputPath = cmdArgs("metadata")
  val outputPath = cmdArgs("output")

  val spark = SparkSession.builder().getOrCreate()

  def getCategory(input: String) = {
    val pattern = """([a-z_]+)""".r
    if (StringUtils.isNotEmpty(input)) (pattern findAllMatchIn input map (_ group 1) toList).headOption.getOrElse("") else null
  }

  def getSalesRank(input: String) = {
    val pattern = """([0-9]+)""".r
    if (StringUtils.isNotEmpty(input)) (pattern findAllMatchIn input map (_ group 1) toList).headOption.getOrElse("") else null
  }

  val udf1 = udf{source: String => getCategory(source)}
  val udf2 = udf{source: String => getSalesRank(source)}

  val dedupData = spark.read.parquet(reviewsInputPath)


  val price = spark.read.parquet(metadataInputPath)
  val salesRankWindow = Window.partitionBy(col("finalTopCategory"), col("brand")).orderBy(asc("salesRankInCategory"), asc("overall"))

  price
    .join(dedupData, "asin")
    .withColumn("rank", rank().over(salesRankWindow))
    .filter(col("rank") <= 5)
    .groupBy("finalTopCategory", "brand")
    .agg(collect_list("asin").alias("asins"))
    .filter("size(asins) == 5")
    .withColumn("rn", row_number().over( Window.partitionBy(col("finalTopCategory")).orderBy("brand")))
    .filter(col("rn") <= 3)
    .write.json(outputPath+"/bestSellers")

}
