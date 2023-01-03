package org.example.schemas

import org.apache.spark.sql.types.StructType

class GooglePSUserReviewsSchema {
  val schema = new StructType().
    add("App", "String", false).
    add("Translated_Review", "String", true).
    add("Sentiment", "String", true).
    add("Sentiment_Polarity", "Double", true).
    add("Sentiment_Subjectivity", "Double", true)
}
