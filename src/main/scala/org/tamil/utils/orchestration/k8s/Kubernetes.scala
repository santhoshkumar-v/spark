package org.tamil.utils.orchestration.k8s

import io.fabric8.kubernetes.api.model.{Pod, Quantity, Secret}
import io.fabric8.kubernetes.client.dsl.PodResource
import io.fabric8.kubernetes.client.{DefaultKubernetesClient, NamespacedKubernetesClient}
import io.minio.MinioClient
import org.tamil.utils.storage.`object`.minio.MinioUtils.putObject

import java.io.{ByteArrayInputStream, InputStream}
import java.util.Base64

case class K8sResource(memory: Quantity,
                       cpu: Quantity)

case class K8sResourceBound(request: K8sResource,
                            limit: K8sResource)

case class K8sResourceQuota(used: K8sResourceBound,
                            hard: K8sResourceBound)

object Kubernetes {

  def intializeClient(namespace: String): NamespacedKubernetesClient = {
    new DefaultKubernetesClient().inNamespace(s"$namespace")
  }

  def getPod(client: Option[NamespacedKubernetesClient],
             podName: String): PodResource[Pod] = {
    if (client.nonEmpty) {
      client.get.pods.withName(s"$podName")
    } else {
      intializeClient("sample_namespace").pods.withName(s"$podName")
    }
  }

  def listPods(client: NamespacedKubernetesClient): Map[String, Pod] = {
    var podConfiguration: Map[String, Pod] = Map()

    client.pods.list().getItems.forEach {
      pod => podConfiguration = Map(pod.getMetadata.getName.trim -> pod)
    }
    podConfiguration
  }


  def validatePod(client: NamespacedKubernetesClient,
                  podName: String): Boolean = {

    if (client.pods().withName(s"$podName").get != null) {
      true
    } else {
      false
    }
  }

  def deletePod(client: NamespacedKubernetesClient,
                podName: String) = {

    val podDeletionStatus = if (validatePod(client, s"$podName")) {
      client.pods().withName(s"$podName").delete().asInstanceOf[Boolean]
    } else {
      println(s"Pod $podName doesnot exist")
      false
    }
  }

  def base64Decoder(encodedString: String): String = {
    new String(Base64.getDecoder.decode(encodedString))

  }

  def base64Encoder(rawString: String): String = {
    Base64.getEncoder.encodeToString(rawString.getBytes)

  }

  def getSecret(client: NamespacedKubernetesClient,
                secretName: String,
                secretKey: String): Option[String] = {
    var rawSecret: Secret = null
    var secret: Option[String] = None
    try {
      rawSecret = client.secrets().withName(s"secretName").get
    } catch {
      case ex: Exception =>
        println(s"Error fetchng k8s secret")
        println(s"Exception: " + ex.getMessage)
    }

    secret = if (rawSecret == null) {
      None
    } else {
      val encryptedSecret = rawSecret.getData.get(s"$secretKey")
      if (encryptedSecret == null) {
        None
      } else {
        Some(base64Decoder(rawSecret.getData.get(s"$secretKey")))
      }
    }
    secret
  }

  def fetchResourceQuota(client: NamespacedKubernetesClient,
                         resourceName: String): K8sResourceQuota = {

    val resourceQuota = client.resourceQuotas.withName(s"$resourceName").get()
    var (hardResourceQuota, usedResourceQuota) = if (resourceQuota != null) {
      val hardResource = resourceQuota.getStatus.getHard
      val usedResource = resourceQuota.getStatus.getUsed
      (K8sResourceBound(K8sResource(hardResource.get("request.memory"), hardResource.get("request.cpu")), K8sResource(hardResource.get("limits.memory"), hardResource.get("limits.cpu"))),
        K8sResourceBound(K8sResource(usedResource.get("request.memory"), usedResource.get("request.cpu")), K8sResource(usedResource.get("limits.memory"), usedResource.get("limits.cpu"))))
    } else {
      (K8sResourceBound(K8sResource(new Quantity("0"), new Quantity("0")), K8sResource(new Quantity("0"), new Quantity("0"))),
        K8sResourceBound(K8sResource(new Quantity("0"), new Quantity("0")), K8sResource(new Quantity("0"), new Quantity("0"))))

    }

    K8sResourceQuota(usedResourceQuota, hardResourceQuota)
  }

  def fetPodLog(client: NamespacedKubernetesClient,
                podName: String,
                prettyLog: Boolean = true): String = {
    if (validatePod(client, s"$podName")) {
      client.pods().withName(s"$podName").getLog(prettyLog)
    } else {
      null
    }

  }

  def watchPodLog(client: NamespacedKubernetesClient,
                  podName: String,
                  tailingLogLinesSize: Int = 0): InputStream = {
    if (validatePod(client, s"$podName")) {
      client.pods().withName(s"$podName").tailingLines(tailingLogLinesSize).watchLog().getOutput
    } else {
      new ByteArrayInputStream("".getBytes())
    }

  }

  def writePodLogToMinio(k8sClient: NamespacedKubernetesClient,
                         minioClient: MinioClient,
                         podName: String,
                         minioBucketName: String,
                         minioObjectPath: String,
                         logFileName: Option[String] = None,
                         forceWrite: Boolean = false,
                         contentType: Option[String] = None): Boolean = {
    val podLog = fetPodLog(k8sClient, s"$podName")
    if (podLog != null) {
      val minioTargetname = if (minioObjectPath.endsWith("/")) {
        if (logFileName.nonEmpty) {
          minioObjectPath + logFileName.get
        } else {
          minioObjectPath + podName
        }
      } else {
        if (logFileName.nonEmpty) {
          minioObjectPath.split("/").dropRight(1).mkString("/") + "/" + logFileName.get
        } else {
          minioObjectPath
        }
      }

      //write into minio using mino client

      putObject(minioClient, podLog.getBytes, minioBucketName, minioTargetname, forceWrite, contentType)

    } else {
      false
    }
  }
}