package sample

import com.twitter.scalding.Args
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import sample.SalesRankAnalyser.args
import sample.StandardiseData.{reviewsInputPath, spark}

object BnCAnalyser extends App{

  val cmdArgs = Args(args)

  val inputPath = cmdArgs("input")
  val outputPath = cmdArgs("output")

  val spark = SparkSession.builder().getOrCreate()

  val price = spark.read.parquet(reviewsInputPath)

  val topCategoryCount = price.select("finalTopCategory").distinct.count()
  println(s"The total number of top categories are: $topCategoryCount")

  price.createOrReplaceTempView("price")

  //top 5 categories overall
  spark.sql(
    """ select count(*) as count1, finalTopCategory from price where finalTopCategory not like 'unknown'\
      |group by finalTopCategory order by count1 desc limit 5""".stripMargin)
    .write.json(outputPath+"/topBrands")

  val catWindow = Window.partitionBy("finalTopCategory").orderBy(desc("count"))

  //top 5 brands in each category
  price
    .filter("brand not in ('','unknown', 'Unknown')")
    .groupBy("finalTopCategory", "brand")
    .count()
    .withColumn("rn", row_number().over(catWindow))
    .filter("rn <= 5")
    .groupBy("finalTopCategory")
    .agg(collect_list("brand"))
    .write.json(outputPath+"/topBrandsInCategories")

  //top 5 brands overall
  spark.sql(
    """ select count(*) as count1, brand from price where brand is not null and brand <> "NaN" \
      |and brand not in ('Unknown','Generic') group by brand order by count1 desc limit 6""".stripMargin)
    .write.json(outputPath+"/topBrands")


  //Brands sorted by categoryCoverage
  import org.apache.spark.sql.expressions.Window
  price
    .filter("brand not in ('','unknown', 'Unknown')")
    .groupBy("finalTopCategory", "brand")
    .count()
    .groupBy("brand")
    .agg(count("finalTopCategory").alias("numCategories"))
    .orderBy(desc("numCategories"))
    .write.json(outputPath+"/brandByCategoryCoverage")




}
