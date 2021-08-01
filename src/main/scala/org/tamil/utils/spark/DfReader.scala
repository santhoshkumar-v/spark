package org.tamil.utils.spark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.tamil.utils.spark.FileSystemReader.readFileSystem
import org.apache.hadoop.fs.Path

object DfReader {

  def readUsingSql(spark: SparkSession,
                   sql: String): DataFrame = {
    if (sql.isEmpty) {
      spark.emptyDataFrame
    } else {
      spark.sql(s"$sql")
    }
  }

  def readFromHive(spark: SparkSession,
                   hiveTableName: String,
                   readConfig: Map[String, String]): DataFrame = {
    if (hiveTableName.isEmpty) {
      spark.emptyDataFrame
    } else {
      if (readConfig.isEmpty) {
        spark.read.table(s"$hiveTableName")
      } else {
        spark.read.options(readConfig).table(s"$hiveTableName")
      }
    }
  }

  def readFromStorage(spark: SparkSession,
                      sourceDataUri: String,
                      sourceDataFormat: String,
                      readConfig: Map[String, String]): DataFrame = {
    if (readFileSystem(spark, s"$sourceDataUri").exists(new Path(sourceDataUri))) {
      spark.read.options(readConfig).format(sourceDataFormat).load(sourceDataUri)
    } else {
      spark.emptyDataFrame
    }
  }
}
