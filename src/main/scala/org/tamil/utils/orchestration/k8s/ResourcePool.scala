package org.tamil.utils.orchestration.k8s

import io.fabric8.kubernetes.api.model.Quantity.getAmountInBytes
import io.fabric8.kubernetes.api.model.ResourceQuota
import org.apache.spark.sql.SparkSession
import org.tamil.utils.orchestration.k8s.Kubernetes.{fetchResourceQuota, intializeClient}

object ResourcePool {

  def fetchResourceReady(spark: SparkSession,
                         namespace: String,
                         resourceName: String,
                         requestedResource: K8sResourceQuota): Boolean = {
    val k8sClient = intializeClient(s"$namespace")
    val usedK8sResource = fetchResourceQuota(k8sClient, s"$resourceName")
    val reqDriverRequestCpu = spark.conf.get("spark.kubernetes.driver.request.cores")
    val reqDriverLimitCpu = spark.conf.get("spark.kubernetes.driver.limit.cores")
    val reqExecutorRequestCpu = spark.conf.get("spark.kubernetes.executor.request.cores")
    val reqExecutorLimitCpu = spark.conf.get("spark.kubernetes.executor.limit.cores")

    val reqRequestMemory = getAmountInBytes(requestedResource.used.request.memory)
    val reqLimitMemory = getAmountInBytes(requestedResource.used.request.memory)
    val reqRequestCpu = getAmountInBytes(requestedResource.used.request.cpu)
    val reqLimitCpu = getAmountInBytes(requestedResource.used.request.cpu)

    val aRequestCpu = getAmountInBytes(usedK8sResource.used.request.cpu)
    val aLimitCpu = getAmountInBytes(usedK8sResource.used.limit.cpu)
    val aRequestMemory = getAmountInBytes(usedK8sResource.used.request.memory)

    val aLimitMemory = getAmountInBytes(usedK8sResource.used.limit.memory)

    val rRequestCpu = getAmountInBytes(requestedResource.used.request.cpu)
    val rLimitCpu = getAmountInBytes(requestedResource.used.limit.cpu)
    val rRequestMemory = getAmountInBytes(requestedResource.used.request.memory)
    val rLimitMemory = getAmountInBytes(requestedResource.used.limit.memory)

    if (aRequestCpu.compareTo(rRequestCpu).==(1) && aRequestMemory.compareTo(rRequestMemory).==(1) && aLimitCpu.compareTo(rLimitCpu).==(1) && aLimitMemory.compareTo(rLimitMemory).==(1)) {
      true
    } else {
      false
    }
  }
}
