package org.apache.spark.volcano

import io.fabric8.kubernetes.api.model.volcano.batch._
import io.fabric8.kubernetes.api.model.{OwnerReferenceBuilder, Pod}
import org.apache.spark.deploy.k8s.Constants
import org.apache.spark.deploy.k8s.submit.KubernetesClientUtils
import org.apache.spark.internal.Logging

import java.util.Collections
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

  def driver(driverPod: Pod,
             queueName: String,
             schedulerName: String,
             namespace: String,
             maxRetry: Int): Job = {
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

    val jobLabels = new java.util.HashMap[String, String]()
    jobLabels.put(Constants.VOLCANO_JOB_NAME_LABEL_KEY, KubernetesClientUtils.DRIVER_VOLCANO_JOB_NAME)
    jobLabels.put(Constants.SPARK_ROLE_LABEL, Constants.SPARK_POD_DRIVER_ROLE)

    // The driver job has only one 1 task and only 1 pod
    // minAvailable must be set to 1 otherwise it goes to "completed" phase
    // as soon as it starts running
    job(KubernetesClientUtils.DRIVER_VOLCANO_JOB_NAME, queueName, schedulerName, namespace, maxRetry)
      .editSpec()
      .withTasks(List(driverTask).asJava)
      .withMinAvailable(1)
      .endSpec()
      .editMetadata()
      .withLabels(jobLabels)
      .endMetadata()
      .build()
  }

  def executor(pod: Pod,
               executorJobName: String,
               queueName: String,
               schedulerName: String,
               namespace: String,
               applicationId: String,
               driverPod: Option[Pod]
              ): Job = {
    logInfo(s"Creating Executor Job with name: $executorJobName")
    val executorTask: TaskSpec = createExecutorTask(pod)

    // Attach the job name as a label on the job
    // This is used in the delete-by-phase operation in VolcanoExecutorPodsAllocator
    val jobLabels = new java.util.HashMap[String, String]()
    jobLabels.put(Constants.VOLCANO_JOB_NAME_LABEL_KEY, executorJobName)
    jobLabels.put(Constants.SPARK_ROLE_LABEL, Constants.SPARK_POD_EXECUTOR_ROLE)
    jobLabels.put(Constants.SPARK_APP_ID_LABEL, applicationId)

    // The executor job is always created with maxRetry = 0
    // this is because executor restarts through K8s or Volcano interfere with
    // spark's internal logic to handle executor loss

    val executorJobBuilder = job(executorJobName, queueName, schedulerName, namespace, 0)
      .editSpec()
      .withMinAvailable(1)
      .withTasks(List(executorTask).asJava)
      .endSpec()
      .editMetadata()
      .withLabels(jobLabels)
      .endMetadata()

    // Add the driver pod as an owner for the executor job so that when the driver pod is deleted
    // all corresponding executor jobs are deleted as well
    if (driverPod.isDefined) {
      val driverPodRef = new OwnerReferenceBuilder()
        .withName(driverPod.get.getMetadata.getName)
        .withApiVersion(driverPod.get.getApiVersion)
        .withUid(driverPod.get.getMetadata.getUid)
        .withKind(driverPod.get.getKind)
        .withController(true)
        .build()

      executorJobBuilder
        .editMetadata()
        .withOwnerReferences(Collections.singletonList(driverPodRef))
        .endMetadata()
        .build()
    } else {
      executorJobBuilder.build()
    }
  }

  private def createExecutorTask(pod: Pod): TaskSpec = {
    // The executor task is always created with 1 replica
    val executorPolicy: LifecyclePolicy = new LifecyclePolicy()
    executorPolicy.setAction("CompleteJob")
    executorPolicy.setEvent("TaskCompleted")

    val executorTask = new TaskSpecBuilder()
      .withReplicas(1)
      .withName(Constants.SPARK_POD_EXECUTOR_ROLE)
      .withNewTemplate()
      .withSpec(pod.getSpec)
      .withMetadata(pod.getMetadata)
      .endTemplate()
      .withPolicies(executorPolicy)
      .build()
    executorTask
  }
}
