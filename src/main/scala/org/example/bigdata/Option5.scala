package org.example.bigdata

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.example.utils.Utils

class Option5 {

  private val STORAGE_FILE = "googleplaystore_metrics";
  var df_4: DataFrame = null

  def option5Executer(data: DataFrame): Unit = {

    // list of columns to help build the final dataframe
    val listCols = List("Genre","App", "Rating", "Average_Sentiment_Polarity")

    // creates a new dataframe separating the array column named genres in multiple rows
    df_4 = data.select(col("App"),explode(col("Genres")).as("Genre"))

    // joins dataframe data and df_4 selecting creating a dataframe with one genre per row by app
    df_4 = data.join(df_4,Seq("App"), "inner").select(listCols.map(m=>col(m)):_*)

    // group dataframe by genre and counts how many apps have it and the corresponded avgs needed
    df_4 = df_4.groupBy("Genre").agg(
      count("App").as("Count"),
      avg("Rating").as("Average_Rating"),
      avg("Average_Sentiment_Polarity").as("Average_Sentiment_Polarity")
    )

    df_4.show()
    df_4.coalesce(1).write.
      option("compression", "gzip").
      parquet(s"data/$STORAGE_FILE");

    new Utils().clearFilesAndDirectories(STORAGE_FILE, ".gz.parquet")
  }
}

