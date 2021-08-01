package org.tamil.utils.spark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions.col

case class SparkMemory(storageParadigm: String,
                       cachedInd: String)

case class Hive(targetSaveMode: String,
                //targetSaveMode: String,
                NatureOfPartition: String,
                partitionKey: String,
                createTableInd: String,
                targetNumPartition: Int,
                storageCredential: Option[ObjectStorageCredential])

case class StorageConfig(dataFormat: String,
                         saveMode: String,
                         partitionKey: String,
                         numPartition: Int,
                         credential: Option[Credential])

case class ObjectStorageCredential(accessKeyId: String,
                                   secretAccessKey: String,
                                   endPointUri: String)

case class DatalakeCredential(cliendId: String,
                              clientSecret: String,
                              grantType: String,
                              scope: String)

case class Credential(objectStorageCred: Option[ObjectStorageCredential],
                      datalakeCred: Option[DatalakeCredential])

object DfWriter {

  def sparkMemoryWriter(spark: SparkSession,
                        sourceDf: DataFrame,
                        targetSparkInMemTable: String,
                        sparkMemoryConfig: SparkMemory): Boolean = {

    val storageParadigm = sparkMemoryConfig.storageParadigm
    val cachedInd = sparkMemoryConfig.cachedInd

    sourceDf.createOrReplaceTempView(s"$targetSparkInMemTable")

    if (cachedInd.equalsIgnoreCase("Y")) {
      if (spark.catalog.isCached(s"targetSparkInMemTable")) {
        spark.catalog.cacheTable(s"targetSparkInMemTable", StorageLevel.fromString(s"$storageParadigm"))
      } else {
        println(s"$targetSparkInMemTable is in cached state ")
      }
    }
    if(spark.catalog.tableExists(targetSparkInMemTable)){true} else{ false}
  }

  def writeIntoStorage(//spark: SparkSession,
                       sourceDataFrame: DataFrame,
                       storageType: String,
                       targetStorageUrl: String,
                       sparkWriterConfig: Map[String, String],
                       writeConfig: StorageConfig) {
    val targetPartitionKey = writeConfig.partitionKey
    val targetPartitionCount = writeConfig.numPartition
    val saveMode = writeConfig.saveMode
    val dataFormat = writeConfig.dataFormat

    //Remove the paramaeter path if it's value=/user/hive/warehouse, since it is cleaned up in HDFS
    if (storageType.equalsIgnoreCase("HDFS")) {
      if (sparkWriterConfig.keys.toList.contains("path") && sparkWriterConfig.getOrElse("path","").equals("/user/hive/warehouse")) {
        sparkWriterConfig - "path"
      }
    }

    if (targetPartitionKey.equalsIgnoreCase("NO_PART")) {
      val targetDf = if (targetPartitionCount.>(0)) {
        sourceDataFrame.repartition(targetPartitionCount)
      } else if (targetPartitionCount.<(0)) {
        sourceDataFrame.coalesce(targetPartitionCount) //not recomemended unless needed, since it causes OOM if not Memory config not configured properly
      } else {
        sourceDataFrame
      }

      targetDf.write.options(sparkWriterConfig).mode(s"saveMode").format(s"$dataFormat").save(s"$targetStorageUrl")
    } else {
      val partitionFields = targetPartitionKey.split(",").toList
      val partitionColumns = partitionFields.map(x => col(x))

      val targetDf = if (targetPartitionCount.>(0)) {
        sourceDataFrame.repartition(targetPartitionCount, partitionColumns: _*)
      } else if (targetPartitionCount.<(0)) {
        sourceDataFrame.coalesce(targetPartitionCount)
      } else {
        sourceDataFrame
      }
      targetDf.
        write.
        options(sparkWriterConfig).
        partitionBy(partitionFields: _*).mode(saveMode).
        format(s"$dataFormat").
        save(targetStorageUrl)
    }

  }
}
