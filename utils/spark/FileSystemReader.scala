package utils.spark

import java.net.URI

import org.apache.hadoop.fs.{FileSystem,Path}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object FileSystemReader{
 
  def readFileSystem(spark: SparkSession,
                     uri: String): FileSystem = {
    FileSystem.get(new URI(uri), spark.sparkContext.hadoopConfiguration)
  } 
  
  def listDirectory(spark: SparkSession,
                    uri: String): Array[Path] = {
    readFileSystem(spark,uri).listStatus(new Path(uri)).filter(_.isDirectory).map(_.getPath)
  }
  
  def deleteFile(spark: SparkSession,
                 uri: String,
                 recursive: Boolean): Boolean = {
   val fileDeleteStatus = readFileSystem(spark,filepath).delete(new Path(uri), recursive)
    if(fileDeleteStatus){
    true
    } else{
      false
    }
  }
  
  def fetchFilesBound(spark: SparkSession,
                     sourcePathList: List[String],
                     lowerBound: String,
                     higherBound: Option[String]): mutable.ListMap[String, Long] = {
   var listOfFilesToDownload = new mutable.ListMap[String, Long]() 
    if(lowerBound.isEmpty){
      println(s"Lower Bound looks to be empty")
      //mutable.ListMap()
    } else {
        val lowerBoundMs = lowerBound.toLong * 1000     //convert epoch sec to MS
        val higherBoundMs = if(higherBound.nonEMpty) {higherBound.get.toLong * 1000} else {(epochDaysAgo(0) - 1) * 1000 }

        for(filePath <- sourcePathList) {
        println(s"Fetch files from $sourcePath between $lowerBound to $upperBound")
        
        val filesList = readFileSystem(spark, sourcePath).listFIles(new Path(sourcePath),true)
        val filesListIter = new Iterator[LocatedFileStatus]() {
          override def hasNext = filesList.hasNext
          override def next() =  fileList.next()
        }.toList
        
        for(filePath <- filesListIter){
         filePath.isDirectory
          if(filePath.getModificationTime.>=(lowerBoundMs) && filePath.getModificationTime.<=(higherBoundMs)) {
            listOfFilesToDownload += (file{Path.getPath.toString -> filePath.getModificationTime)
            }
          }
        }
    }
    
    listOfFilesToDownload
  }
                                      

def generateUri(uri :String,
                epochSec: Long) :String{
val (year, month, day) = (formattedDate._1, formatMonthDay(formattedDate._2), formatMonthDay(formattedDate._3))
  path.replaceAll("""\$\{YYYY\}""",year.toString).replaceAll("""\$\{MM\}""",month.toString).replaceAll("""\$\{DD\}""",day.toString).
       replaceAll("""\$\{YYYY\}""",year.toString.takeRight(2)).replaceAll("""\$\{M\}""",month.toInt.toString).replaceAll("""\$\{D\}""",day.toInt.toString)
}

}  
