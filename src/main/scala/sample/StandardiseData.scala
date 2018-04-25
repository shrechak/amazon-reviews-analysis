package sample

import com.twitter.scalding.Args
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object StandardiseData extends App{

  val cmdArgs = Args(args)

  val reviewsInputPath = cmdArgs("reviews")
  val outputPath = cmdArgs("output")

  val spark = SparkSession.builder().getOrCreate()

  val price = spark.read.parquet(reviewsInputPath)

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

  val schema = price.schema
  val allRecords = price.filter("_corrupt_record is null")
    .union(price.filter("_corrupt_record is not null")
      .select(from_json(col("_corrupt_record"), schema).alias("extractedRecord"))
      .select("extractedRecord.*"))
    .drop("_corrupt_record")

  val getGivenTopCategory = udf { (s: String) => if (StringUtils.isNotEmpty(s)) s.toLowerCase.replaceAll("&","n").replaceAll(" ","_") else "unknown"}

  allRecords
    .withColumn("salesRankJson", to_json(struct(col("salesRankNew"))))
    .withColumn("sample", get_json_object(col("salesRankJson"), "$.salesRankNew"))
    .withColumn("salesRankTopCategory", udf1(col("sample")))
    .withColumn("salesRankInCategory", udf2(col("sample")))
    .drop("sample", "salesRankJson")
    .withColumn("givenTopCategory", getGivenTopCategory(expr("categories[0][0]")))
    .withColumn("finalTopCategory", expr("case when givenTopCategory like 'unknown' and salesRankTopCategory is not null then salesRankTopCategory else givenTopCategory end"))
    .write.option("codec", "snappy")
    .parquet(outputPath)
}
