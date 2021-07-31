package utils.storage.object.minio

import java.io.ByteArrayInputStream
import java.lang.Iterable
import java.net.URLDecoder
import java.util

import io.minio.messages.{DeleteObject, Item}
import io.minio.CopySource
import io.minio._
import okhttp3.Headers

import utils.storage.object.ObjectStorageUtils.ObjectInfo

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
  
  def uploadFile(minioClient: MinioClinet,
                sourceFileName: String,
                targetBucket: String,
                targetObjectName: String): Boolean = {
   if(new File(sourceFileName).exists){
     //if Minio path end with /
   val targetObjectPath = if(targetObjectName.endsWith("/"){
     targetObjectName + sourceFile.split("/").last
   } else {
     targetObjectName
   }
                             
   minioCLient.uploadObject(UploadObjectArgs.builder().bucket(targetBucket).`object`(targetObjectName).filename(sourceFileName).build()
   if(validateObject(minioClient,targetBucket,targetObjectName){
     true
   }else{
     false
   }
}
    
  def putObject(minioClient: MinioClient,
                sourceDataStream: Array[Byte],
                targetBucket: String,
                targetObjectName: String,
                forceWrite: Boolen = false,
                contentType: Option[String] = None): Boolean = {
   if(validateObject(minioClient,targetBuckdt,targetObjectName) && !forceWrite){
     println("Target object exists already !!!")
     false
   } else{
      if(contentType.isEmpty){
      minioClient.putObject(PutObjectARgs.builder().bucket(targetBucket).`object`(targetObjectName).
                            stream(new ByteArrayInputSTream(sourceDataStream), sourceDataStream.size, -1).build())
      }else{
      minioClient.putObject(PutObjectARgs.builder().bucket(targetBucket).`object`(targetObjectName).
                            stream(new ByteArrayInputSTream(sourceDataStream), sourceDataStream.size, -1).
                            contentType(contentType.get).build())
      }
   }
    
}
  
  def deleteTempObjects(minioCLient: MinioClient,
                       bucket: String,
                       folderPrefix: String,
                       tempPathPrefix: String*): Boolean = {
   //intialise linkedList to hold object to be deleted 
   val deleteObjectsList = new util.LinkedList[DeleteObject]
   val objectPathPrefix = if(folderPrefix.trim.takeRight(1).equals("/")){
     folderPrefix.dropRight(1)
   }else{
     folderPrefix
   }
   
   if(bucket.nonEmpty && folderPrefix.nonEmpty){
     val tempObjectPrefix = if(tempPathPrefix.isEmpty){
      // Set the  temp path prefix if not set
     List("_temporary",".spark-staging",".hive-staging") 
     }else{
     deletePathPrefix
     }
     
       
       //iterate over each of the temp object prefix
       tempObjectPrefix.
       foreach{ objectPrefix =>
         if(validateObject(minioClient,bucket,s"$objectPathPrefix/$objectPrefix")){
           listVersionedBucket(minioClient, bucket,s"$objectPathPrefix/$objectPrefix").
           forEach{ fqObjectName =>
             val formattedObjectName = URLDecoder.decode(fqObjectName.get().objectName(),"UTF-8")
             deleteObjectsList.add(new DeleteObject(formattedObjectName,fqObjectName.get().versionId())
                   }
                 }
              }

     //CHeck if list of obejcts to be deleted is empty
     // NonEMpty unavaliable
    if(!deleteObjectsList.isEmpty){
      if(removeObjects(minioClient,bucket,deleteObjectsList)){
        true
      } else{
        println("Failed to remove object")
        false
      }
    } else{
      println("No object marked for delete")
      false
    }
  } else{
    println("Object Path $bucket$folderPrefix doesn't exist")
    false
  }
}                            

// COpy object and remove Source object
def moveObject(minioClient: MinioClient,
               sourceBucket: String,
               sourceObject: String,
               targetBucket: String,
               targetObject: Option[String] = None): Boolean = {
  
val moveObjectStatus: ObjectWriteResponse = if(targetObject.isEmpty{
     if(sourceBucket != targetBucket){
       minioClient.copyObject(
        CopyObjectArgs.builder().
        source(CopySource.builder().bucket(sourceBucket).`object`(sourceObject).build()).
        bucket(targetBucket).`object`(sourceObject).build())
     } else{
     //Creatning dummy write response, since source and target buckt are same, so asusming move is not requeired
      new ObjectWriteResponse(new Headers(Array("")),targetBucket,null,sourceObject,null,null)
    }
  } else{
      if(sourceBucket == targetBucket && sourceObject != targetObject ){
         minioClient.copyObject(
           CopyObjectArgs.builder().
           source(CopySource.builder().bucket(sourceBucket).`object`(sourceObject).build()).
           bucket(targetBucket).`object`(targetObject).build())
       } else{
          println("Source and target seems to be same, hence skipping")
          new ObjectWriteResponse(new Headers(Array("")),targetBucket,null,sourceObject,null,null)
       }
  }
                                               
if(moveObjectStatus.versionId().nonEmpty && moveObjectStatus.etag().nonEmpty){
  val sourceObjectInfo = parseObjectUrl(s"$sourceBucket/sourceObject")
 // val sourceObjectPath =  if(sourceObject.takeRight(1).eq("/")){
 //   sourceObject.dropRight(1)
 // } else{
 //   sourceObject
  //}
//  val sourceObjectName = sourceObjectPath.split("/").last
 // val sourceObjectDeleteStatus = deleteTempObjects(minioClient,sourcebucket,sourceObjectName)
  
   val sourceObjectDeleteStatus = deleteTempObjects(minioClient,sourcebucket,sourceObjectInfo.objectPath,sourceObjectInfo.objectName)
   if(sourceObjectDeleteStatus) {
    println("Failed to remove source object $sourceBucket/$sourceObject")
     true
   } else{
     false
   }
} else{
 println("Failed to move object $sourceBucket/$sourceObject into $targetBucket/$targetObject")
  false
}

}  
