package com.sparkbyexamples.spark.dataframe.examples

import org.apache.spark.sql.SparkSession

object DataFrameEmptyCheck extends App {

  val spark:SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExample")
    .getOrCreate()

  val df = spark.emptyDataFrame

  println(df.isEmpty)
  println(df.rdd.isEmpty())
  //df.head() on empty data frame throws java.util.NoSuchElementException: next on empty iterator
  //println(df.head())
  println()
}
