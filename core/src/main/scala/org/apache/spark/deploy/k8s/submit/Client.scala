package org.apache.spark.deploy.k8s.submit

import io.fabric8.kubernetes.api.model.{ContainerBuilder, PodBuilder}
import io.fabric8.kubernetes.client.{KubernetesClient, Watch}
import io.fabric8.kubernetes.client.Watcher.Action
import org.apache.spark.deploy.k8s.Constants.{ENV_SPARK_CONF_DIR, SPARK_CONF_DIR_INTERNAL, SPARK_CONF_VOLUME_DRIVER}
import org.apache.spark.deploy.k8s.KubernetesDriverConf
import org.apache.spark.deploy.k8s.KubernetesUtils.addOwnerReference

import scala.collection.JavaConverters._
import scala.util.control.Breaks.{break, breakable}
import scala.util.control.NonFatal

private[spark] class Client(
  conf: KubernetesDriverConf,
  builder: KubernetesDriverBuilder,
  kubernetesClient: KubernetesClient,
  watcher: LoggingPodStatusWatcher
) extends AbstractClient {

  def run(): Unit = {
    val resolvedDriverSpec = builder.buildFromFeatures(conf, kubernetesClient)
    val configMapName = KubernetesClientUtils.configMapNameDriver
    val confFilesMap = KubernetesClientUtils.buildSparkConfDirFilesMap(configMapName,
      conf.sparkConf, resolvedDriverSpec.systemProperties)
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

    val resolvedDriverPod = new PodBuilder(resolvedDriverSpec.pod.pod)
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
    val driverPodName = resolvedDriverPod.getMetadata.getName

    var watch: Watch = null
    val createdDriverPod = kubernetesClient.pods().create(resolvedDriverPod)
    try {
      val otherKubernetesResources = resolvedDriverSpec.driverKubernetesResources ++ Seq(configMap)
      addOwnerReference(createdDriverPod, otherKubernetesResources)
      kubernetesClient.resourceList(otherKubernetesResources: _*).createOrReplace()
    } catch {
      case NonFatal(e) =>
        kubernetesClient.pods().delete(createdDriverPod)
        throw e
    }
    val sId = Seq(conf.namespace, driverPodName).mkString(":")
    breakable {
      while (true) {
        val podWithName = kubernetesClient
          .pods()
          .withName(driverPodName)
        // Reset resource to old before we start the watch, this is important for race conditions
        watcher.reset()
        watch = podWithName.watch(watcher)

        // Send the latest pod state we know to the watcher to make sure we didn't miss anything
        watcher.eventReceived(Action.MODIFIED, podWithName.get())

        // Break the while loop if the pod is completed or we don't want to wait
        if(watcher.watchOrStop(sId)) {
          watch.close()
          break
        }
      }
    }
  }
}

