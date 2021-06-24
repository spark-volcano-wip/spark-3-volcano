package org.apache.spark.volcano

import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.api.model.volcano.batch._
import org.apache.spark.deploy.k8s.Constants
import org.apache.spark.deploy.k8s.submit.KubernetesClientUtils
import org.apache.spark.internal.Logging

import scala.collection.JavaConverters._

object JobCreator extends Logging {

  def job(jobName: String, queueName: String, schedulerName: String, namespace: String, maxRetry: Int): JobBuilder = {
    new JobBuilder()
      .withApiVersion("batch.volcano.sh/v1alpha1")
      .withNewSpec()
      .withQueue(queueName)
      .withSchedulerName(schedulerName)
      .withMaxRetry(maxRetry)
      .endSpec()
      .withNewMetadata()
      .withName(jobName)
      .withNamespace(namespace)
      // The uid is created in the basicDiverFeatureStep and has to be attached to the job to enable the ownerreference
      // .withUid(pod.getMetadata().getUid())
      .endMetadata()
  }

  def driver(driverPod: Pod, queueName: String, schedulerName: String, namespace: String, maxRetry: Int): Job = {
    logInfo(s"Creating Driver Job with name: ${KubernetesClientUtils.DRIVER_VOLCANO_JOB_NAME}")

    // DriverTask lifecyclePolicy to delay the job completion until the task
    // has completed
    val driverPolicy: LifecyclePolicy = new LifecyclePolicy()
    driverPolicy.setAction("CompleteJob")
    driverPolicy.setEvent("TaskCompleted")

    val driverTask: TaskSpec = new TaskSpecBuilder()
      .withReplicas(1)
      .withName(Constants.SPARK_POD_DRIVER_ROLE)
      .withNewTemplate()
      .withSpec(driverPod.getSpec)
      .withMetadata(driverPod.getMetadata)
      .endTemplate()
      .withPolicies(driverPolicy)
      .build()

    // The driver job has only one 1 task and only 1 pod
    // minAvailable must be set to 1 otherwise it goes to "completed" phase
    // as soon as it starts running
    job(KubernetesClientUtils.DRIVER_VOLCANO_JOB_NAME, queueName, schedulerName, namespace, maxRetry)
      .editSpec()
      .withTasks(List(driverTask).asJava)
      .withMinAvailable(1)
      .endSpec()
      .build()
  }

  def executor(pod: Pod, executorJobName: String, queueName: String, schedulerName: String, namespace: String, maxRetry: Int): Job = {
    logInfo(s"Creating Executor Job with name: $executorJobName")
    val executorTask: TaskSpec = createExecutorTask(pod)

    job(executorJobName, queueName, schedulerName, namespace, maxRetry)
      .editSpec()
      .withMinAvailable(0)
      .withTasks(List(executorTask).asJava)
      .endSpec()
      .build()
  }

  private def createExecutorTask(pod: Pod) = {
    // The executor task is always created with zero replicas
    val executorTask = new TaskSpecBuilder()
      .withReplicas(0)
      .withName(Constants.SPARK_POD_EXECUTOR_ROLE)
      .withNewTemplate()
      .withSpec(pod.getSpec)
      .withMetadata(pod.getMetadata)
      .endTemplate()
      .build()
    executorTask
  }

  def getExecutor(job: Job): TaskSpec = {
    // todo we have to retrieve it based on the task name
    job.getSpec
      .getTasks
      .asScala
      .find(task => task.getName == Constants.SPARK_POD_EXECUTOR_ROLE)
      .get
  }
}
