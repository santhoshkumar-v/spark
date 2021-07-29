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
  
}
