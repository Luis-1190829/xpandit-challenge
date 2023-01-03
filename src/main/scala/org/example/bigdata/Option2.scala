package org.example.bigdata

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc
import org.example.schemas.GooglePlayStoreSchema
import org.example.utils.Utils

class Option2 {

  private val FILE_PATH = "data/googleplaystore.csv"
  private val SCHEMA = new GooglePlayStoreSchema()
  private val STORAGE_FILE = "best_apps"

  def option2Executer(): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    // reads data according to the defined schema
    var data = spark.read.
      option("header", true).
      option("dateFormat","MMM dd, yyyy").
      schema(SCHEMA.schema).
      csv(FILE_PATH)

    // replaces NaN values with 0
    data = data.na.fill(0,Array("Rating"))

    // selects the apps with 4.0 rating or greater and sort the dataframe in descending
    // order according to the rating
    data = data.filter(data("Rating") >= 4.0).
      sort(desc("Rating"))

    // write dataframe data to a single csv file
    data.coalesce(1).write.
      option("header", value = true).
      option("delimiter" , "§").
      csv(s"data/$STORAGE_FILE")

    // saves csv file with the pretended name deleting all non necessary files and directories
    new Utils().clearFilesAndDirectories(STORAGE_FILE,".csv");
  }
}
