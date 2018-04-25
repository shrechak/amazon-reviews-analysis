package sample

import com.twitter.scalding.Args
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object DataRepartitioner extends App{

  val cmdArgs = Args(args)

  val reviewsInputPath = cmdArgs("reviews")
  val metadataInputPath = cmdArgs("metadata")
  val outputPath = cmdArgs("output")
  val numPartitions = cmdArgs("num-partitions").toInt

  val spark = SparkSession.builder().getOrCreate()

  def readGzippedJsonData(path: String) = {
    spark.read
      .option("inferSchema", "true")
      .option("codec", "gzip")
      .json(path)
  }

  //GZIP is not splittable.
  //Converting to snappy+parquet for better reading

  val reviewsInput = readGzippedJsonData(reviewsInputPath)

  reviewsInput
    .repartition(numPartitions)
    .write.option("codec", "snappy")
    .parquet(outputPath)

  val metadataInput = readGzippedJsonData(metadataInputPath)

  val salesRankSchema = StructType(List(
    StructField("appliances", LongType, true),
    StructField("arts_crafts_sewing", LongType, true),
    StructField("automotive", LongType, true),
    StructField("baby", LongType, true),
    StructField("beauty", LongType, true),
    StructField("books", LongType, true),
    StructField("camera_n_photo", LongType, true),
    StructField("cell_phones_accessories", LongType, true),
    StructField("clothing", LongType, true),
    StructField("computers_n_accessories", LongType, true),
    StructField("electronics", LongType, true),
    StructField("gift_cards_store", LongType, true),
    StructField("grocery_n_gourmet_food", LongType, true),
    StructField("health_n_personal_care", LongType, true),
    StructField("home_n_kitchen", LongType, true),
    StructField("home_improvement", LongType, true),
    StructField("industrial_n_scientific", LongType, true),
    StructField("jewelry", LongType, true),
    StructField("kitchen_n_dining", LongType, true),
    StructField("magazines", LongType, true),
    StructField("movies_n_tv", LongType, true),
    StructField("music", LongType, true),
    StructField("musical_instruments", LongType, true),
    StructField("office_products", LongType, true),
    StructField("patio_lawn_n_garden", LongType, true),
    StructField("pet_supplies", LongType, true),
    StructField("prime_pantry", LongType, true),
    StructField("shoes", LongType, true),
    StructField("software", LongType, true),
    StructField("sports_n_outdoors", LongType, true),
    StructField("toys_n_games", LongType, true),
    StructField("video_games", LongType, true),
    StructField("watches", LongType, true)
  ))

  metadataInput
    .withColumn("salesRankNew", col("salesRank").cast(salesRankSchema))
    .drop("salesRank")
    .repartition(numPartitions)
    .withColumnRenamed("salesRankNew", "salesRank")
    .write.option("codec", "snappy")
    .parquet(outputPath)
}
