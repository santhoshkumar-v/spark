package org.tamil.utils.spark

import java.net.URI
import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path}
import org.apache.spark.sql.SparkSession
import org.tamil.utils.spark.Catalog.readFromCatalog
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.collection.mutable

import org.tamil.utils.spark.FileSystemReader.readFileSystem

case class hiveFqName(database: String, tableName: String)

object Hive {

  def getSchemaFromMetastore(spark: SparkSession,
                             tableName: String): StructType = {
    val hiveTable = parseHiveTableName(tableName)

    readFromCatalog(spark).getTable(hiveTable.database, hiveTable.tableName).schema
  }

  def parseHiveTableName(fqTableName: String): hiveFqName = {
    if (fqTableName.split('.').length.==(2)) {
      hiveFqName(fqTableName.split('.')(0), fqTableName.split('.')(1))
    } else {
      hiveFqName("", fqTableName.split('.')(0))
    }
  }

  def readFromCsvOneLineHeader(spark: SparkSession,
                               tableName: String,
                               header: Boolean): DataFrame = {

    val tableLocation = getHiveTableLocation(spark, tableName)
    val hiveTableLocation = if (readFileSystem(spark, tableLocation.toString).isDirectory(new Path(tableLocation))) {
      s"$tableLocation/*"
    } else {
      tableLocation
    }
    spark.read.option("header", header.toString).csv(s"$hiveTableLocation")

  }

  def getHiveTableLocation(spark: SparkSession,
                           fqTableName: String): URI = {
    val hiveTableName = parseHiveTableName(fqTableName)

    readFromCatalog(spark).getTable(hiveTableName.database, hiveTableName.tableName).storage.locationUri.get
  }


  def createSparkViewFromCsv(spark: SparkSession,
                             fqTableName: String): Boolean = {
    val dbName = fqTableName.split('.')(0)
    val tableName = fqTableName.split('.')(1)
    val tableLocation = getHiveTableLocation(spark, fqTableName)
    val hiveTableLocation = if (org.apache.hadoop.fs.FileSystem.get(tableLocation, spark.sparkContext.hadoopConfiguration).isDirectory(new Path(tableLocation))) {
      s"${tableLocation.toString}/*"
    } else {
      tableLocation.toString
    }

    val sparkViewNameFromHive = s"${dbName}_${tableName}"
    val columns = spark.read.table(fqTableName).columns

    val filedDelimiter = spark.sharedState.externalCatalog.
      getTable(dbName, tableName).
      storage.
      properties.
      getOrElse[String]("field.delim", "")

    if (!filedDelimiter.isEmpty) {
      spark.read.
        option("header", "true").
        option("delimiter", filedDelimiter).
        csv(hiveTableLocation).toDF(columns: _*).
        createOrReplaceTempView(s"$sparkViewNameFromHive")
    } else {
      sys.exit(1)
    }
    spark.catalog.
      tableExists(sparkViewNameFromHive)
  }

  // cast all columns
  //val schemaFromHive = getSchemaFromMetastore(spark, schema.get)
  //spark.read.locat("").selectExpr(schemaFromHive.map(field => s"CAST(${field.dataType.sql}) ${field.name}"): _*)

  //cast individual comuns alone
  //df.select(df.columns.map(filed => if (fieldSchema.keys.toList.contains(filed.toString)) {
  //    from_json(col(field), StructType.fromDDL(filedSchema.getOrElse(filed, ""))).as(field)
  //  } else {
  //    col(filed)
  //  }
  //  ): _*)

}
