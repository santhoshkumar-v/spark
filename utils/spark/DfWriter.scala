package utils.spark

import org.apache.spark.sql{DataFrame, SparkSesison}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions.col

case class SparkMemory(storageParadigm: String,
                       cachedInd: String)

case class Hive(targetSaveMode: String,
                targetSaveMode: String,
                NatureOfPartition: String,
                partitionKey: String,
                createTableInd: String,
                targetNumPartition: Int,
                storageCredential : Option[ObjectStorageCredential])

case class StorageConfig(dataFormat: String,
                         saveMode: String,
                         partitionKey: String,
                         numPartition: Int,
                         credential: Option[Credential])

case class ObjectStorageCredential(accesKeyId: String,
                                   secretAccessKey: String,
                                   endPointUri: String)

case class DatalakeCredential(cliendId: String,
                              clientSecret: String,
                              grantType: String,
                              scope: String)

case class Credential(objectStorageCred: Option[ObjectStorageCredential],
                      datalakeCred: Option[DatalakeCredential])

object DfWriter{
  
 def sparkMemoryWriter(spark: SparkSession,
                      sourceDf: DataFrame,
                      targetSparkInMemTable: String,
                      sparkMemoryConfig: SparkMemory) = {
  
  val storageParadigm = sparkMemoryConfig.storageParadigm
  val cachedInd = sparkMemoryConfig.isCached
  
  sourceDf.createOrReplaceTempView(s"$targetSparkInMemTable")
  
  if(cachedInd.equalsIgnoreCase("Y"){
    if(spark.catalog.isCached(s"targetSparkInMemTable")) {
      spark.catalog.cacheTable(s"targetSparkInMemTable",StorageLevel.fromString(s"$storageParadigm"))
    } else{
      println(s"$targetSparkInMemTable is in cached state ")
    }
  }
}  

def writeIntoStorage(spark: SparkSession,
                     sourceDataFrame: DataFrame,
                     storageType: String,
                     targetStorageUrl: String,
                     sparkWriterConfig: Map[String, String],
                     writeConfig: StorageConfig): = {
  val targetPartitionKey = writeConfig.partitionKey
  val targetPartitionCount = writeConfig.numPartition
  val saveMode =  writeConfig.saveMode
  val dataFormat = writeConfig.dataFormat
  
  //Remove the paramaeter path if it's value=/user/hive/warehouse, since it is cleaned up in HDFS
  if(storageType.equalsIgnoreCase("HDFS")){
    if(sparkWriterConfig.keys.toList.contains("path") && sparkWriterConfig.get("path").equals(Some("/user/hive/warehouse"))){
      sparkWriterConfig - "path"
    }
  }
  
 if(targetType.equalsIgnoreCase("NO_PART"){
   val targetDf = if(targetPartitionCount.>(0)){
     sourceDataFrame.repartition(targetPartitionCount)
   } else if(targetPartitionCount.<(0)){
     sourceDataFrame.coalesce(targetPartitionCount)    //not recomemendedunless needed, since it causes OOM if not memeconfig not configured properly
   } else {
     sourceDataFrame
   }  
  
  targetDf.write.options(sparkWriterConfig).mode(saveMode).format(dataFormat).save(targetStorageUrl)
} else{
 val partitionFields = targetPartitionKey.split(",").toList
   val targetDf = if(targetPartitionCount.>(0)){
     val partitionColumns = partitionFields.map(x => col(x))
     sourceDataFrame.repartition(targetPartitionCount,partitionColumns :_*)
   } else if(targetPartitionCount.<(0)){
     sourceDataFrame.coalesce(targetPartitionCount)
   } else{
     sourceDataFrame
   }
   targetDf.write.options(sparkWriterConfig).partitionBy(partitionColumns: _*).mode(saveMode).format(dataFormat).save(targetStorageUrl)
 }
     
}
