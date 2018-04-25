package sample

import com.twitter.scalding.Args
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SalesRankAnalyser extends App{

  val cmdArgs = Args(args)

  val inputPath = cmdArgs("input")
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


  val dedupData = spark.read.parquet("s3://indix-users/shreya/required/partitioned-metadata/")
  val dedupData2 = dedupData.withColumn("salesRankJson", to_json(struct(col("salesRankNew"))))
    .withColumn("sample", get_json_object(col("salesRankJson"), "$.salesRankNew"))
    .withColumn("topCategory", udf1(col("sample")))
    .withColumn("salesRankInCategory", udf2(col("sample")))
    .drop("sample", "salesRankJson")

  val topCategoryCount = dedupData2.select("topCategory").distinct.count()
  println(s"The total number of top categories are: $topCategoryCount")

  import org.apache.spark.sql.expressions.Window

  val salesRankWindow = Window.partitionBy(col("topCategory"), col("brand")).orderBy(asc("salesRankInCategory"))


  //top 5 ranked ASINs in B+C category
  dedupData2
    .withColumn("rank", rank().over(salesRankWindow))
    .filter(col("rank") <= 5)
    .filter("topCategory is not null")
    .groupBy("topCategory", "brand")
    .agg(collect_list("asin"))
    .write.json(outputPath+"/top5")

}
