package utils.spark

import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.spark.fileSystemReader
import org.apache.hadoop.fs.Path

object DfReader {

  def readUsingSql(spark: SparkSession,
       sql: String): DataFrame = {
    if(sql.isEMpty){
      spark.emptyDataFrame
    } else{
    spark.sql(s"$sql")
    }
  }
  
  def readFromHive(spark: SparkSession,
                  hiveTableName: String,
                  readConfig: Map[String, String]): DataFrame = {
    if(hiveTableName.isEmpty){
      spark.emptyDataframe
    } else{
      if(readConfig.isEmpty) {
        spark.read.table(s"$hiveTableName")
      } else{
        spark.read.options(readConfig).table(s"$hiveTableName")
      }
    }
  
    def readFromStorage(spark: SParkSession,
                        sourceDataUri: String,
                        sourceFormat: Sting,
                        readConfig: Map[String, String]): DataFrame = {
     if(fileSystemReader(spark, sourceDataUri).exists(new Path(sourceDataUri)){
       spark.read.options(readConfig).format(sourceDataFormat).load(sourceDataUri)
     }else {
       spark.emptyDataFrame
     }
    }
}
