package org.tamil.utils.storage.`object`

case class BucketInfo(bucketName: String, objectName: String)

case class ObjectInfo(bucketName: String,objectPath: String, objectName: String)

object ObjectStorageUtils{
  
  def parseBucket(objectPath: String): BucketInfo = {
   if( objectPath.startsWith("s3://") ||  objectPath.startsWith("s3n://") || objectPath.startsWith("s3a://")){
      BucketInfo(objectPath.split("/").drop(2)(0),objectPath.split("/").drop(3).mkString("/"))
   }else if( objectPath.startsWith("s3:/") ||  objectPath.startsWith("s3n:/") ||  objectPath.startsWith("s3a:/")){
      BucketInfo(objectPath.split("/")(1),objectPath.split("/").drop(2).mkString("/"))
   }else{
      BucketInfo(objectPath.split("/")(0),objectPath.split("/").drop(1).mkString("/"))
   }
    
  }
  
  def parseObjectUrl(objectPath: String): ObjectInfo = {
   if( objectPath.startsWith("s3://") ||  objectPath.startsWith("s3n://")  || objectPath.startsWith("s3a://")){
      ObjectInfo(objectPath.split("/").drop(2)(0),objectPath.split("/").drop(3).dropRight(1).mkString("/"),objectPath.split("/").drop(3).last)
   }else if( objectPath.startsWith("s3:/") ||  objectPath.startsWith("s3n:/") || objectPath.startsWith("s3a:/")){
      ObjectInfo(objectPath.split("/")(1),objectPath.split("/").drop(2).dropRight(1).mkString("/"),objectPath.split("/").drop(2).last)
   }else{
      ObjectInfo(objectPath.split("/")(0),objectPath.split("/").drop(1).dropRight(1).mkString("/"),objectPath.split("/").drop(1).last)
   }
    
  }
  
}  
