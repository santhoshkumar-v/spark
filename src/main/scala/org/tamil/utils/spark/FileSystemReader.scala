package org.tamil.utils.spark

import java.net.URI
import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import org.tamil.utils.DateUtils.{formatDate, formatMonthDay}
case class Bound(lowerBound: String,   upperBound :String)

object FileSystemReader {

  def readFileSystem(spark: SparkSession,
                     uri: String): FileSystem = {
    FileSystem.get(new URI(uri), spark.sparkContext.hadoopConfiguration)
  }

  def listDirectory(spark: SparkSession,
                    uri: String): Array[Path] = {
    readFileSystem(spark, uri).listStatus(new Path(uri)).filter(_.isDirectory).map(_.getPath)
  }

  def deleteFile(spark: SparkSession,
                 uri: String,
                 recursive: Boolean): Boolean = {
    val fileDeleteStatus = readFileSystem(spark, uri).delete(new Path(uri), recursive)
    if (fileDeleteStatus) {
      true
    } else {
      false
    }
  }

  def fetchFilesBound(spark: SparkSession,
                      sourcePathList: List[String],
                      lowerBound: String,
                      upperBound: Option[String]): mutable.ListMap[String, Long] = {
    var listOfFilesToDownload = new mutable.ListMap[String, Long]()
    if (lowerBound.isEmpty) {
      println(s"Lower Bound looks to be empty")
      //mutable.ListMap()
    } else {
      val lowerBoundMs = lowerBound.toLong * 1000 //convert epoch sec to MS
      val higherBoundMs = if (upperBound.nonEmpty) {
        upperBound.get.toLong * 1000
      } else {
        //(epochDaysAgo(0) - 1) * 1000
        0
      }

      for (filePath <- sourcePathList) {
        println(s"Fetch files from $filePath between $lowerBound to $upperBound")

        val filesList = readFileSystem(spark, filePath).listFiles(new Path(filePath), true)
        val filesListIter = new Iterator[LocatedFileStatus]() {
          override def hasNext = filesList.hasNext

          override def next() = filesList.next()
        }.toList

        for (filePath <- filesListIter) {
          filePath.isDirectory
          if (filePath.getModificationTime.>=(lowerBoundMs) && filePath.getModificationTime.<=(higherBoundMs)) {
            listOfFilesToDownload += (filePath.getPath.toString -> filePath.getModificationTime)
          }
        }
      }
    }

    listOfFilesToDownload
  }


  def generateUri(uri: String,
                  epochSec: Long): String ={
    val formattedDate = formatDate(epochSec)
    val (year, month, day) = (formattedDate.year, formatMonthDay(formattedDate.month), formatMonthDay(formattedDate.day) )
    uri.replaceAll ("""\$\{YYYY\}""", year.toString).
    replaceAll ("""\$\{MM\}""", month.toString).
    replaceAll ("""\$\{DD\}""", day.toString).
    replaceAll ("""\$\{YY\}""", year.toString.takeRight(2) ).
    replaceAll ("""\$\{M\}""", month.toInt.toString).
    replaceAll ("""\$\{D\}""", day.toInt.toString)
  }

}
