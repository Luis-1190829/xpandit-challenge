package org.example.schemas

import org.apache.spark.sql.types.{StructType}

class GooglePlayStoreSchema {
  val schema = new StructType().
    add("App","String",false).
    add("Category","String",true).
    add("Rating","Double",true).
    add("Reviews","Long",true).
    add("Size","String",true).
    add("Installs","String",true).
    add("Type","String",true).
    add("Price","Double",true).
    add("Content Rating","String",true).
    add("Genres","String",true).
    add("Last Updated","Date",true).
    add("Current Ver","String",true).
    add("Android Ver","String",true)
}
