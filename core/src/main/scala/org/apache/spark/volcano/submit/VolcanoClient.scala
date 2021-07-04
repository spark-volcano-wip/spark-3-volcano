package org.apache.spark.volcano.submit

import io.fabric8.kubernetes.api.model._
import io.fabric8.kubernetes.api.model.volcano.batch.Job
import io.fabric8.kubernetes.client.Watcher.Action
import io.fabric8.kubernetes.client.{KubernetesClient, Watch}
import org.apache.spark.deploy.k8s.Config.KUBERNETES_VOLCANO_DELETE_DRIVER
import org.apache.spark.deploy.k8s.Constants.{ENV_SPARK_CONF_DIR, SPARK_CONF_DIR_INTERNAL, SPARK_CONF_VOLUME_DRIVER}
import org.apache.spark.deploy.k8s.KubernetesDriverConf
import org.apache.spark.deploy.k8s.submit.{AbstractClient, KubernetesClientUtils, KubernetesDriverBuilder}
import org.apache.spark.util.Utils
import org.apache.spark.volcano.VolcanoOperator

import java.util.Collections
import scala.collection.JavaConverters._
import scala.util.control.Breaks.{break, breakable}
import scala.util.control.NonFatal

class VolcanoClient(
                     conf: KubernetesDriverConf,
                     builder: KubernetesDriverBuilder,
                     kubernetesClient: KubernetesClient,
                     watcher: LoggingJobStatusWatcher
) extends AbstractClient {

  def run(): Unit = {
    val resolvedDriverSpec = builder.buildFromFeatures(conf, kubernetesClient)
    val configMapName = KubernetesClientUtils.configMapNameDriver

    val confFilesMap = KubernetesClientUtils.buildSparkConfDirFilesMap(configMapName, conf.sparkConf, resolvedDriverSpec.systemProperties)
    val configMap = KubernetesClientUtils.buildConfigMap(configMapName, confFilesMap)

    // The include of the ENV_VAR for "SPARK_CONF_DIR" is to allow for the
    // Spark command builder to pickup on the Java Options present in the ConfigMap
    val resolvedDriverContainer = new ContainerBuilder(resolvedDriverSpec.pod.container)
      .addNewEnv()
      .withName(ENV_SPARK_CONF_DIR)
      .withValue(SPARK_CONF_DIR_INTERNAL)
      .endEnv()
      .addNewVolumeMount()
      .withName(SPARK_CONF_VOLUME_DRIVER)
      .withMountPath(SPARK_CONF_DIR_INTERNAL)
      .endVolumeMount()
      .build()

    val resolvedDriverPod: Pod = new PodBuilder(resolvedDriverSpec.pod.pod)
      .editSpec()
      .addToContainers(resolvedDriverContainer)
      .addNewVolume()
      .withName(SPARK_CONF_VOLUME_DRIVER)
      .withNewConfigMap()
      .withItems(KubernetesClientUtils.buildKeyToPathObjects(confFilesMap).asJava)
      .withName(configMapName)
      .endConfigMap()
      .endVolume()
      .endSpec()
      .build()

    val volcanoOperator: VolcanoOperator = new VolcanoOperator(kubernetesClient, conf.sparkConf)

    val driverJob: Job = volcanoOperator.createDriver(resolvedDriverPod)

    var watch: Watch = null

    try {
      val otherKubernetesResources = resolvedDriverSpec.driverKubernetesResources ++ Seq(configMap)
      addOwnerReference(driverJob, otherKubernetesResources)
      kubernetesClient.resourceList(otherKubernetesResources: _*).createOrReplace()
    } catch {
      // When you create job. You expect the pod to be created. But if for some reason it crashes. You have to destroy the job
      case NonFatal(e) =>
        volcanoOperator.delete(driverJob)
        throw e
    }

    // Construct the resource identifier for the driver job
    val sId = Seq(conf.namespace, driverJob.getMetadata.getName).mkString(":")

    breakable {
      // Keep iterating if the driver pod is created
      while (true) {
        // Lookup the current state of the driver job
        logInfo(s"Looking for driver job: ${driverJob.getMetadata.getName}")
        val currentJob = volcanoOperator.jobClient.withName(driverJob.getMetadata.getName)
        watcher.reset()

        if (currentJob != null) {
          watch = currentJob.watch(watcher)
          watcher.eventReceived(Action.MODIFIED, currentJob.get)
        }
        // Break the while loop if the pod is completed or we don't want to wait
        if(watcher.watchOrStop(sId)) {
          watch.close()
          if (currentJob != null && conf.sparkConf.get(KUBERNETES_VOLCANO_DELETE_DRIVER)) {
            logInfo(s"Deleting driver job ${driverJob.getMetadata.getName} ")
            Utils.tryLogNonFatalError {
              volcanoOperator.jobClient.delete(driverJob)
            }
          }
          break
        }
      }
    }
  }

  def addOwnerReference(job: Job, resources: Seq[HasMetadata]): Unit = {
    if (job != null) {
      val reference = new OwnerReferenceBuilder()
        .withName(job.getMetadata.getName)
        .withApiVersion(job.getApiVersion)
        .withUid(job.getMetadata.getUid)
        .withKind(job.getKind)
        .withController(true)
        .build()
      resources.foreach { resource =>
        val originalMetadata = resource.getMetadata
        originalMetadata.setOwnerReferences(Collections.singletonList(reference))
      }
    }
  }
}
