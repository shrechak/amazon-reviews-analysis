package sample

import com.twitter.scalding.Args
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

class PriceAnalyser extends App{

  val cmdArgs = Args(args)

  val inputPath = cmdArgs("input")
  val outputPath = cmdArgs("output")

  val spark = SparkSession.builder().getOrCreate()

  val price = spark.read.parquet(inputPath)

  val topCategoryCount = price.select("finalTopCategory").distinct.count()
  println(s"The total number of top categories are: $topCategoryCount")

  price.createOrReplaceTempView("price")

  val priceWindow = Window.partitionBy(col("finalTopCategory"), col("brand")).orderBy(asc("price"))
  val maxPrice = max("price").over(priceWindow).as("maxPrice")
  val minPrice = min("price").over(priceWindow).as("minPrice")
  val avgPrice = avg("price").over(priceWindow).as("avgPrice")

  private val products: Dataset[Row] = price
    .filter("brand is not null and brand not like ''")
    .select(expr("*"), avgPrice)
    .select("finalTopCategory", "brand", "maxPrice", "minPrice", "avgPrice")
    .distinct


  //get cheapest brands in category
  products
    .withColumn("cheapestBrandRn", row_number().over(Window.partitionBy(col("finalTopCategory")).orderBy(asc("avgPrice"))))
    .filter("cheapestBrandRn <= 3")
    .groupBy("finalTopCategory")
    .agg(collect_list("brand").alias("brandList"))
    .write.json(outputPath+"/cheapest")

  //get costliest brands in category
  products
    .withColumn("costliestBrandRn", row_number().over(Window.partitionBy(col("finalTopCategory")).orderBy(desc("avgPrice"))))
    .filter("cheapestBrandRn <= 3")
    .groupBy("finalTopCategory")
    .agg(collect_list("brand").alias("brandList"))
    .write.json(outputPath+"/costliest")

  //number of stable brands per category(no outliers)
  products.groupBy("finalTopCategory", "brand")
    .agg(stddev_pop("price").alias("std"))
    .filter("std is not null and std < 3")
    .groupBy("finalTopCategory")
    .count()
    .write.json(outputPath+"/stableBrandCounts")

  //brands with highest outliers per category
  products.groupBy("finalTopCategory", "brand")
    .agg(stddev_pop("price").alias("std"))
    .filter("std is not null and std > 3")
    .withColumn("rn", row_number().over(Window.partitionBy("finalTopCategory").orderBy(asc("std"))))
    .filter("rn <= 3")
    .groupBy("finalTopCategory")
    .agg(collect_list("brand").alias("brandList"))
    .write.json(outputPath+"/highestOutliers/")
}
