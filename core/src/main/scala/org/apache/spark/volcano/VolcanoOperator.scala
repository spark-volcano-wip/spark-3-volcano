package org.apache.spark.volcano

import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.api.model.volcano.batch.Job
import io.fabric8.kubernetes.client.{DefaultKubernetesClient, KubernetesClient}
import org.apache.spark.SparkConf
import org.apache.spark.deploy.k8s.Config
import org.apache.spark.internal.Logging
import org.apache.spark.volcano.dsl.VolcanoJobOperationsImpl


class VolcanoOperator(kubernetesClient: KubernetesClient, sparkConf: SparkConf) extends Logging {

  val jobClient: VolcanoJobOperationsImpl =
    new VolcanoJobOperationsImpl(
      // the instance of KubernetesClient has to be explicitly cast into the
      // Child class DefaultKubernetesClient to extract the okHTTPClient.
      // This is fine because it is always a DefaultKubernetesClient - see
      // SparkKubernetesClientFactory.createKubernetesClient
      kubernetesClient.asInstanceOf[DefaultKubernetesClient].getHttpClient,
      kubernetesClient.getConfiguration
    )

  def createDriver(driverPod: Pod): Job = {
    val resolvedDriverJob = JobCreator.driver(
      driverPod,
      sparkConf.get(Config.KUBERNETES_VOLCANO_QUEUE),
      sparkConf.get(Config.KUBERNETES_VOLCANO_SCHEDULER),
      sparkConf.get(Config.KUBERNETES_NAMESPACE),
      sparkConf.get(Config.KUBERNETES_VOLCANO_MAX_RETRY)
    )
    jobClient.create(resolvedDriverJob)
  }

  def getPods(podName: String): Pod = {
    kubernetesClient.pods.withName(podName).get()
  }

  def createExecutors(executorJobName: String, executorPod: Pod, applicationId: String, driverPod: Option[Pod]): Job = {
    val createdJob = JobCreator.executor(
      executorPod,
      executorJobName,
      sparkConf.get(Config.KUBERNETES_VOLCANO_QUEUE),
      sparkConf.get(Config.KUBERNETES_VOLCANO_SCHEDULER),
      sparkConf.get(Config.KUBERNETES_NAMESPACE),
      applicationId,
      driverPod
    )

    jobClient.create(createdJob)
  }

  def delete(job: Job): Boolean = {
    jobClient.delete(job)
  }
}
