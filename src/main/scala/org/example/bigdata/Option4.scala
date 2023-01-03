package org.example.bigdata

import org.apache.spark.sql.DataFrame
import org.example.utils.Utils

class Option4 {

  private val STORAGE_FILE = "googleplaystore_cleaned";

  var data: DataFrame = null

  def option4Executer(df_1: DataFrame, df_3: DataFrame): Unit = {

    // left joins df_3 and df_1 through the app column
    data = df_3.join(df_1,Seq("App"), "left")

    data.coalesce(1).write.
      option("compression","gzip").
      parquet(s"data/$STORAGE_FILE");

    new Utils().clearFilesAndDirectories(STORAGE_FILE,".gz.parquet")

  }
}
