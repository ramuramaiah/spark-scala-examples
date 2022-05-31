package com.sparkbyexamples.spark.dataframe

import org.apache.spark.sql.SparkSession

object FromCSVMultiline extends App {

  val spark:SparkSession = SparkSession.builder()
    .master("local[3]")
    .appName("SparkByExamples.com")
    .getOrCreate()


  val df = spark.read
    .option("header",true)
    .option("delimiter",",")
    .option("multiLine",true)
    .option("quotes","\"")
    .csv("src/main/resources/address-multiline.csv")

  val columns = Seq("Id","Address","City","State","Zipcode")
  
  import spark.sqlContext.implicits._
  
  //replace the new line char with space
  val df2 = df.map(row=>{
    val address = row.getString(1).replace("\n", " ")
    (row.getString(0),address,row.getString(2),row.getString(3),row.getString(4))
  }).toDF(columns:_*)
  
  df2.show(false)
}
