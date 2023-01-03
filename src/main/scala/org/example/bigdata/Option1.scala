package org.example.bigdata

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.example.schemas.GooglePSUserReviewsSchema

class Option1 {

  private val FILE_PATH = "data/googleplaystore_user_reviews.csv"
  var df_1: DataFrame = null

  def option1Executer(): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    // reads data bypassing quotes and double quotes
    df_1 = spark.read.
    option("header", value = true).
    option("quote", "\"").
    option("escape", "\"").
    csv(FILE_PATH)

    // cast Sentiment_Polarity column to double
    df_1 = df_1.withColumn("Sentiment_Polarity",col("Sentiment_Polarity").cast(DoubleType))

    // group the dataframe by app and calculate de Sentiment_Polarity avg and names the avg column to
    // Average_Sentiment_Polarity in the end changes the null values to 0
    df_1 = df_1.groupBy("App").
      avg("Sentiment_Polarity").
      withColumnRenamed("avg(Sentiment_Polarity)","Average_Sentiment_Polarity").
      na.fill(0)

    // df_1.show(300)

  }
}
