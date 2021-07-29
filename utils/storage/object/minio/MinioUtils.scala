package utils.storage.object.minio

import java.io.ByteArrayInputStream
import java.lang.Iterable
import java.net.URLDecoder
import java.util

import io.minio.messages.{DeleteObject, Item}
import io.minio.CopySource
import io.minio._
import okhttp3.Headers

object MinioUtils{
  
  def initialiseMinioClient(accessKeyId: String,
                            secretAccessKey: String,
                            endPoint: String): MinioClient = {
  MinioClient.builder().endpoint(s"$endPoint").credentials(s"$accessKeyId",s"$secretAccessKey").buld()
  }
  
  def validateBucket(minioClient: MinioClient,
                     bucketName: String): Boolean = {
   try{
     minioCLient.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build())
   } catch{
     case exce: Exception => false
   }
  }
  
  def listVersionedBucket(minioCLient: MinioClient,
                          bucketName: String,
                          folderPrefix: Option[String]) : Iterable[Result[Item]] = {
   if(validateBucket(minioClient,bucketName){
      if(folderPrefix.isEmpty){
      minioClient.listObjects(ListObjectArgs.builder().bucket(bucketName).recursive(true).build())
      } else{
      minioClient.listObjects(ListObjectArgs.builder().bucket(bucketName).prefix(folderPrefix).recursive(true).build())
      }
    } else{
    prinln(s"Mentioned bucket doesn't exist")
     //intialise empty iterator, so output type is matched
     minioClient.listObjects(ListObjectArgs.builder().bucket("dummy-bucket").prefix("~").build())
    }
  }
  
  def validateObject(minioClient: MinioClient,
                     bucketName: String,
                     folderPrefix: String): Boolean = {
   if(validateBucket(minioClient,bucketName) &&  folderPrefix.nonEmpty) {
     if(listVersionedBucket(minioClient,bucketName,folderPrefix).iterator().hasNext){
       true
     } else{
       false
     }
  }
  
  def copyObject(minioClient: MinioClient,
                 sourceBucket: String,
                 sourceObjectName: String,
                 targetBucket: String,
                 targetObjectName: Option[String] =None): Boolean = {
   val copyObjectStatus: ObjectWriteResponse = if (targetObjectName.isEmpty){
   minioClient.copyObject(CopyObjectArgs.builder().
                          source(CopySource.builder().bucket(sourceBucket).`object`(sourceObjectName).build()).
                          bucket(targetBucket).`object`(sourceObjectName.get).build())
   } else{
   minioClient.copyObject(CopyObjectArgs.builder().
                          source(CopySource.builder().bucket(sourceBucket).`object`(sourceObjectName).build()).
                          bucket(targetBucket).`object`(targetObjectName.get).build())
   }
  
    if(copyObjectStatus.versionId().nonEmpty && copyObjectStatus.etag().nonEmpty){
      true
    } else {
      false
    }
  
  }
  
  def removeObjects(minioClient: MinioClient,
                    bucketName: String,
                    objectsToRemove: util.LinkedList[DeleteObject]): Boolean = {
   println(s"Number of objects to be removed: "+ objectsToRemove.size())
    val objectRemovalStatus = minioClient.removeObjects(objectsToRemove.builder().bucket(bucketName).objects(objectsToRemove).build())
    //Actually objects are removed here
    objectRemovalStatus.foreach(println)
    
    val interatableResult = objectRemovalStatus.iterator()
    while(interatableResult.hasNext){
      val error = interatableResult.next().get()
      println(s"Error while removing object "+error.objectName()+" due to "+error.message())
      false
    }
    //If the iterator returns nothing, then remove operation is success
    true
  }
  
  def uploadFile(){
  
  }
    
  def putObject(minioClient: MinioClient,
                sourceDataStream: Array[Byte],
                targetBucket: String,
                targetObjectName: String,
                forceWrite: Boolen = false,
                contentType: Option[String] = None): Boolean = {
   
    
  }
  
}  
