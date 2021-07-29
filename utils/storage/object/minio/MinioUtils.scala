package utils.storage.object.minio

import java.io.ByteArrayInputStream
import java.lang.Iterable
import java.net.URLDecoder
import java.util

import io.minio.messages.{DeleteObject, Item}
import io.minio.CopySource
import io.minio._

object MinioUtils{
  
  def initialiseMinioClient(accessKeyId: String,
                            secretAccessKey: String,
                            endPoint: String): MinioClient = {
  MinioClient.builder().endpoint(s"$endPoint").credentials(s"$accessKeyId",s"$secretAccessKey").buld()
  }
  
  def copyObject(minioClient: MinioClient,
                 sourceBucket
}  
