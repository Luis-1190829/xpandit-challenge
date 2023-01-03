package org.example.bigdata


import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.example.schemas.GooglePlayStoreSchema

class Option3 {

  private val FILE_PATH = "data/googleplaystore.csv"
  private val SCHEMA = new GooglePlayStoreSchema()
  var df_3: DataFrame = null

  def option3Executer(): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    // reads data according to the defined schema
    df_3 = spark.read.
      option("header", true).
      option("dateFormat", "MMM dd, yyyy").
      schema(SCHEMA.schema).
      csv(FILE_PATH)

    // fill reviews columns null values as 0 and replaces NaN values as nulls
    df_3 = df_3.na.fill(0,Array("Reviews"))
    df_3 = df_3.select(replaceEmptyCols(df_3.columns): _*)

    // transform string column genre to array of strings
    df_3 = df_3.withColumn("Genres", split(col("Genres"), ";").cast("array<string>"))

    // creates a new dataframe data1 with a column named app and a column named categories
    // that have all categories of each app
    val data1 = df_3.groupBy("App").agg(collect_set("Category").as("Categories"))

    // new dataframe with app column and its max reviews value
    val data2 = df_3.groupBy("App").max("Reviews").withColumnRenamed("max(Reviews)","Reviews")

    // joins dataframes data2 and df_3 to have no duplicate values in the column app
    df_3 = data2.join(df_3,Seq("App","Reviews"), "inner")
    df_3 = df_3.dropDuplicates("App")

    df_3 = df_3.join(data1,Seq("App"), "inner").drop("Category")

    df_3 = df_3.withColumnRenamed("Content Rating","Content_Rating")
    df_3 = df_3.withColumnRenamed("Last Updated","Last_Updated")
    df_3 = df_3.withColumnRenamed("Current Ver","Current_Version")
    df_3 = df_3.withColumnRenamed("Android Ver","Minimum_Android_Version")

    df_3 = df_3.withColumn("Price",col("Price")*0.9)
    df_3 = df_3.withColumn("Size",
      when(col("Size").endsWith("M"), col("Size").substr(lit(0), length(col("Size")) - 1).as("Double"))
      .when(col("Size")endsWith("k"), col("Size").substr(lit(0), length(col("Size")) - 1).as("Double")/ 1024)
      .otherwise(-1).cast("Double"))

  }

  // method to replace all NaN values in a dataframe
  private def replaceEmptyCols(columns: Array[String]): Array[Column] = {
    columns.map(c => {
      when(col(c) === "NaN", null).otherwise(col(c)).alias(c)
    })
  }
}
