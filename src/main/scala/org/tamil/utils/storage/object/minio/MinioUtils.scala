package org.tamil.utils.storage.`object`.minio

import java.net.URLDecoder
import java.util

import java.io.{ByteArrayInputStream, File}
import org.tamil.utils.storage.`object`.ObjectStorageUtils.parseObjectUrl
import io.minio.messages.{DeleteObject, Item}
import okhttp3.Headers
import io.minio._

object MinioUtils {

  def initialiseMinioClient(accessKeyId: String,
                            secretAccessKey: String,
                            endPoint: String): MinioClient = {
    MinioClient.builder().endpoint(s"$endPoint").credentials(s"$accessKeyId", s"$secretAccessKey").build()
  }

  def validateBucket(minioClient: MinioClient,
                     bucketName: String): Boolean = {
    try {
      minioClient.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build())
    } catch {
      case exce: Exception => false
    }
  }

  def listVersionedBucket(minioClient: MinioClient,
                          bucketName: String,
                          folderPrefix: Option[String]): java.lang.Iterable[Result[Item]] = {
    if (validateBucket(minioClient, bucketName)) {
      if (folderPrefix.isEmpty) {
        minioClient.listObjects(ListObjectsArgs.builder().bucket(bucketName).recursive(true).build())
      } else {
        minioClient.listObjects(ListObjectsArgs.builder().bucket(bucketName).prefix(folderPrefix.get).recursive(true).build())
      }
    } else {
      println(s"Mentioned bucket doesn't exist")
      //intialise empty iterator, so output type is matched
      minioClient.listObjects(ListObjectsArgs.builder().bucket("dummy-bucket").prefix("~").build())
    }
  }

  def validateObject(minioClient: MinioClient,
                     bucketName: String,
                     folderPrefix: String): Boolean = {
    if (validateBucket(minioClient, bucketName) && folderPrefix.nonEmpty) {
      if (listVersionedBucket(minioClient, s"$bucketName", Some(folderPrefix)).iterator().hasNext) {
        true
      } else {
        false
      }
    } else {
      println(s"Bucket doesn't exist")
      false
    }
  }

  def copyObject(minioClient: MinioClient,
                 sourceBucket: String,
                 sourceObjectName: String,
                 targetBucket: String,
                 targetObjectName: Option[String] = None): Boolean = {
    val copyObjectStatus: ObjectWriteResponse = if (targetObjectName.isEmpty) {
      minioClient.copyObject(CopyObjectArgs.builder().
        source(CopySource.builder().bucket(sourceBucket).`object`(sourceObjectName).build()).
        bucket(targetBucket).`object`(sourceObjectName).build())
    } else {
      minioClient.copyObject(CopyObjectArgs.builder().
        source(CopySource.builder().bucket(sourceBucket).`object`(sourceObjectName).build()).
        bucket(targetBucket).`object`(targetObjectName.get).build())
    }

    if (copyObjectStatus.versionId().nonEmpty && copyObjectStatus.etag().nonEmpty) {
      true
    } else {
      false
    }

  }

  def removeObjects(minioClient: MinioClient,
                    bucketName: String,
                    objectsToRemove: util.LinkedList[DeleteObject]): Boolean = {
    println(s"Number of objects to be removed: " + objectsToRemove.size())
    val objectRemovalStatus = minioClient.removeObjects(RemoveObjectsArgs.builder().bucket(bucketName).objects(objectsToRemove).build())
    //Actually objects are removed here
    objectRemovalStatus.forEach(println)

    val interatableResult = objectRemovalStatus.iterator()
    while (interatableResult.hasNext) {
      val error = interatableResult.next().get()
      println(s"Error while removing object " + error.objectName() + " due to " + error.message())
      false
    }
    //If the iterator returns nothing, then remove operation is success
    true
  }

  def uploadFile(minioClient: MinioClient,
                 sourceFileName: String,
                 targetBucket: String,
                 targetObjectName: String): Boolean = {
    if (new File(sourceFileName).exists) {
      //if Minio path end with /
      val targetObjectPath = if (targetObjectName.endsWith("/")) {
        targetObjectName + sourceFileName.split("/").last
      } else {
        targetObjectName
      }

      minioClient.uploadObject(UploadObjectArgs.builder().bucket(targetBucket).`object`(targetObjectName).filename(sourceFileName).build())
      if (validateObject(minioClient, targetBucket, targetObjectName)) {
        true
      } else {
        false
      }
    } else {
      println(s"Source file $sourceFileName doesn't exist")
      false
    }
  }

  def putObject(minioClient: MinioClient,
                sourceDataStream: Array[Byte],
                targetBucket: String,
                targetObjectName: String,
                forceWrite: Boolean = false,
                contentType: Option[String] = None): Boolean = {
    if (validateObject(minioClient, targetBucket, targetObjectName) && !forceWrite) {
      println("Target object exists already !!!")
      false
    } else {
      if (contentType.isEmpty) {
        minioClient.putObject(PutObjectArgs.builder().bucket(targetBucket).`object`(targetObjectName).
          stream(new ByteArrayInputStream(sourceDataStream), sourceDataStream.length, -1).build())
      } else {
        minioClient.putObject(PutObjectArgs.builder().bucket(targetBucket).`object`(targetObjectName).
          stream(new ByteArrayInputStream(sourceDataStream), sourceDataStream.length, -1).
          contentType(contentType.get).build())
      }
      if (validateObject(minioClient, targetBucket, targetObjectName)) {
        true
      } else {
        false
      }
    }
  }

  def deleteTempObjects(minioClient: MinioClient,
                        bucket: String,
                        folderPrefix: String,
                        tempPathPrefix: String*): Boolean = {
    //intialise linkedList to hold object to be deleted
    val deleteObjectsList = new util.LinkedList[DeleteObject]
    val objectPathPrefix = if (folderPrefix.trim.takeRight(1).equals("/")) {
      folderPrefix.dropRight(1)
    } else {
      folderPrefix
    }

    if (bucket.nonEmpty && folderPrefix.nonEmpty) {
      val tempObjectPrefix = if (tempPathPrefix.isEmpty) {
        // Set the  temp path prefix if not set
        List("_temporary", ".spark-staging", ".hive-staging")
      } else {
        tempPathPrefix
      }


      //iterate over each of the temp object prefix
      tempObjectPrefix.
        foreach { objectPrefix =>
          if (validateObject(minioClient, bucket, s"$objectPathPrefix/$objectPrefix")) {
            listVersionedBucket(minioClient, bucket, Some(s"$objectPathPrefix/$objectPrefix")).
              forEach { fqObjectName =>
                val formattedObjectName = URLDecoder.decode(fqObjectName.get().objectName(), "UTF-8")
                deleteObjectsList.add(new DeleteObject(formattedObjectName, fqObjectName.get().versionId()))
              }
          }
        }

      //CHeck if list of obejcts to be deleted is empty
      // NonEMpty unavaliable
      if (!deleteObjectsList.isEmpty) {
        if (removeObjects(minioClient, bucket, deleteObjectsList)) {
          true
        } else {
          println("Failed to remove object")
          false
        }
      } else {
        println("No object marked for delete")
        false
      }
    } else {
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

    val moveObjectStatus: ObjectWriteResponse = if (targetObject.isEmpty) {
      if (sourceBucket != targetBucket) {
        minioClient.copyObject(
          CopyObjectArgs.builder().
            source(CopySource.builder().bucket(sourceBucket).`object`(sourceObject).build()).
            bucket(targetBucket).`object`(sourceObject).build())
      } else {
        //Creatning dummy write response, since source and target buckt are same, so asusming move is not requeired
        new ObjectWriteResponse(new Headers(Array("")), targetBucket, null, sourceObject, null, null)
      }
    } else {
      if (sourceBucket == targetBucket && targetObject.nonEmpty && sourceObject != targetObject.get) {
        minioClient.copyObject(
          CopyObjectArgs.builder().
            source(CopySource.builder().bucket(sourceBucket).`object`(sourceObject).build()).
            bucket(targetBucket).`object`(targetObject.get).build())
      } else {
        println("Source and target seems to be same, hence skipping")
        new ObjectWriteResponse(new Headers(Array("")), targetBucket, null, sourceObject, null, null)
      }
    }

    if (moveObjectStatus.versionId().nonEmpty && moveObjectStatus.etag().nonEmpty) {
      val sourceObjectInfo = parseObjectUrl(s"$sourceBucket/sourceObject")
      // val sourceObjectPath =  if(sourceObject.takeRight(1).eq("/")){
      //   sourceObject.dropRight(1)
      // } else{
      //   sourceObject
      //}
      //  val sourceObjectName = sourceObjectPath.split("/").last
      // val sourceObjectDeleteStatus = deleteTempObjects(minioClient,sourceBucket,sourceObjectName)

      val sourceObjectDeleteStatus = deleteTempObjects(minioClient, s"$sourceBucket", s"${sourceObjectInfo.objectPath}", s"${sourceObjectInfo.objectName}")
      if (sourceObjectDeleteStatus) {
        println("Failed to remove source object $sourceBucket/$sourceObject")
        true
      } else {
        false
      }
    }
    else {
      println("Failed to move object $sourceBucket/$sourceObject into $targetBucket/$targetObject")
      false
    }

  }
}
