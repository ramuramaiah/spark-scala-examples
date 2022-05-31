package com.sparkbyexamples.spark.dataframe.examples

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.hadoop.io.IOUtils
import java.io.IOException
import org.apache.spark.sql.{SaveMode, SparkSession}

object SaveSingleFile extends App{

  val spark:SparkSession = SparkSession.builder()
    .master("local[3]")
    .appName("SparkByExamples.com")
    .getOrCreate()

  val df = spark.read.option("header",true)
    .csv("src/main/resources/address.csv")
  df.repartition(1)
    .write.mode(SaveMode.Overwrite).csv("/tmp/address")


  val hadoopConfig = new Configuration()
  val hdfs = FileSystem.get(hadoopConfig)

  val srcPath=new Path("/tmp/address")
  val destPath= new Path("/tmp/address_merged.csv")
  val srcFile=FileUtil.listFiles(new File("c:/tmp/address"))
    .filterNot(f=>f.getPath.endsWith(".csv"))(0)
  //Copy the CSV file outside of Directory and rename
  FileUtil.copy(srcFile,hdfs,destPath,true,hadoopConfig)
  //Remove Directory created by df.write()
  hdfs.delete(srcPath,true)
  //Removes CRC File
  hdfs.delete(new Path("/tmp/.address_merged.csv.crc"),true)

  // Merge Using Haddop API
  df.repartition(1).write.mode(SaveMode.Overwrite)
    .csv("/tmp/address-tmp")
  val srcFilePath=new Path("/tmp/address-tmp")
  val destFilePath= new Path("/tmp/address_merged2.csv")
  copyMerge(hdfs, srcFilePath, hdfs, destFilePath, true, hadoopConfig)
  //Remove hidden CRC file if not needed.
  hdfs.delete(new Path("/tmp/.address_merged2.csv.crc"),true)
  
  def copyMerge(
    srcFS: FileSystem, srcDir: Path,
    dstFS: FileSystem, dstFile: Path,
    deleteSource: Boolean, conf: Configuration
  ): Boolean = {

  if (dstFS.exists(dstFile)) {
    throw new IOException(s"Target $dstFile already exists")
  }

  // Source path is expected to be a directory:
  if (srcFS.getFileStatus(srcDir).isDirectory) {

    val outputFile = dstFS.create(dstFile)
    try {
      srcFS
        .listStatus(srcDir)
        .sortBy(_.getPath.getName)
        .collect {
          case status if status.isFile =>
            val inputFile = srcFS.open(status.getPath)
            try { IOUtils.copyBytes(inputFile, outputFile, conf, false) }
            finally { inputFile.close() }
        }
    } finally { outputFile.close() }

    if (deleteSource) srcFS.delete(srcDir, true) else true
  }
    else false
  }

}
