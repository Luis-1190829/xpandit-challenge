package org.example.ui

import org.apache.spark.sql.SparkSession

object Main extends App{
  private val mainMenuLoop = new MainMenu()
  val spark = SparkSession.
    builder().
    master("local").
    appName("Google play store data").
    getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  mainMenuLoop.mainMenuLoop()
  spark.stop()
}
