/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.volcano.scheduler

import io.fabric8.kubernetes.api.model.PodBuilder
import io.fabric8.kubernetes.api.model.volcano.batch.Job
import io.fabric8.kubernetes.client.KubernetesClient
import org.apache.spark.deploy.k8s.Config.KUBERNETES_VOLCANO_ADD_EXECUTOR_OWNER_REFERENCE
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.KubernetesConf
import org.apache.spark.deploy.k8s.submit.KubernetesClientUtils
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.scheduler.cluster.k8s._
import org.apache.spark.util.{Clock, Utils}
import org.apache.spark.volcano.VolcanoOperator
import org.apache.spark.{SecurityManager, SparkConf}

import scala.collection.JavaConverters._
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.collection.mutable

class VolcanoExecutorPodsAllocator(
    conf: SparkConf,
    secMgr: SecurityManager,
    executorBuilder: KubernetesExecutorBuilder,
    kubernetesClient: KubernetesClient,
    snapshotsStore: ExecutorPodsSnapshotsStore,
    clock: Clock,
    volcanoOperator: VolcanoOperator,
    clusterMode: Boolean,
    executorIdsToJobs: mutable.HashMap[Long, Job]
) extends ExecutorPodsAllocator(
  conf: SparkConf,
  secMgr: SecurityManager,
  executorBuilder: KubernetesExecutorBuilder,
  kubernetesClient: KubernetesClient,
  snapshotsStore: ExecutorPodsSnapshotsStore,
  clock: Clock
) {

  override def kubernetesDriverPodName: Option[String] = {
    // For volcano, we need to supply the driver job created in spark-submit machine
    // and propagate to the driver as System property
    if (clusterMode) {
      val prefix = System.getProperty(DRIVER_VOLCANO_JOB_NAME_KEY)
      val podName = s"${prefix}-${SPARK_POD_DRIVER_ROLE}-0"
      Some(podName)
    } else {
      super.kubernetesDriverPodName
    }
  }

  private def createJob(executorId: Long, rpId: Int, applicationId: String): Job = {
    // Create one Volcano Job per executor
    val applicationId = conf.getOption("spark.app.id").getOrElse("spark-application-" + System.currentTimeMillis)
    val executorConf = KubernetesConf.createExecutorConf(conf, executorId.toString, applicationId, driverPod, rpId)

    val resolvedExecutorSpec = executorBuilder.buildFromFeatures(executorConf, secMgr,
      kubernetesClient, rpIdToResourceProfile(rpId))
    val executorPod = resolvedExecutorSpec.pod
    val podWithAttachedContainer = new PodBuilder(executorPod.pod)
      .editOrNewSpec()
      .addToContainers(executorPod.container)
      .endSpec()
      .build()

    // The executor job name is the combination of prefix and executor ID
    val jobNamePrefix: String = KubernetesClientUtils.getVolcanoExecutorJobNamePrefix(clusterMode)
    val jobName: String = s"$jobNamePrefix-$executorId"
    val executorOwner = if (conf.get(KUBERNETES_VOLCANO_ADD_EXECUTOR_OWNER_REFERENCE)) driverPod else None
    val executorJob: Job = volcanoOperator.createExecutors(
      jobName, podWithAttachedContainer, applicationId, executorOwner)
    logInfo(s"Created executor job $jobName")
    executorJob
  }

  override def onNewSnapshots(
      applicationId: String,
      schedulerBackend: KubernetesClusterSchedulerBackend,
      snapshots: Seq[ExecutorPodsSnapshot]): Unit = {
    val k8sKnownExecIds = snapshots.flatMap(_.executorPods.keys)
    logDebug(s"k8sKnownExecIds: $k8sKnownExecIds, " +
      s"schedulerKnownNewlyCreatedExecs: ${schedulerKnownNewlyCreatedExecs.keys}")
    newlyCreatedExecutors --= k8sKnownExecIds
    schedulerKnownNewlyCreatedExecs --= k8sKnownExecIds

    // transfer the scheduler backend known executor requests from the newlyCreatedExecutors
    // to the schedulerKnownNewlyCreatedExecs
    val schedulerKnownExecs = schedulerBackend.getExecutorIds().map(_.toLong).toSet
    val knownExecIds = newlyCreatedExecutors.filterKeys(schedulerKnownExecs.contains).mapValues(_._1)
    schedulerKnownNewlyCreatedExecs ++= knownExecIds
    newlyCreatedExecutors --= schedulerKnownNewlyCreatedExecs.keySet

    // For all executors we've created against the API but have not seen in a snapshot
    // yet - check the current time. If the current time has exceeded some threshold,
    // assume that the pod was either never created (the API server never properly
    // handled the creation request), or the API server created the pod but we missed
    // both the creation and deletion events. In either case, delete the missing pod
    // if possible, and mark such a pod to be rescheduled below.
    val currentTime = clock.getTimeMillis()
    val timedOut = newlyCreatedExecutors.flatMap { case (execId, (_, timeCreated)) =>
      if (currentTime - timeCreated > podCreationTimeout) {
        Some(execId)
      } else {
        logInfo(s"Executor with id $execId was not found in the Kubernetes cluster since it" +
          s" was created ${currentTime - timeCreated} milliseconds ago.")
        None
      }
    }

    if (timedOut.nonEmpty) {
      logWarning(s"Executors with ids ${timedOut.mkString(",")} were not detected in the" +
        s" Kubernetes cluster after $podCreationTimeout ms despite the fact that a previous" +
        " allocation attempt tried to create them. The executors may have been deleted but the" +
        " application missed the deletion event.")

      newlyCreatedExecutors --= timedOut
//      if (shouldDeleteExecutors) {
//        Utils.tryLogNonFatalError {
//          kubernetesClient
//            .pods()
//            .withLabel(SPARK_APP_ID_LABEL, applicationId)
//            .withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE)
//            .withLabelIn(SPARK_EXECUTOR_ID_LABEL, timedOut.toSeq.map(_.toString): _*)
//            .delete()
//        }
//      }
      // volcano equivalent of the above steps - delete volcano jobs for the timed out executors
      if (shouldDeleteExecutors) {
        // Select the Jobs for the executor IDs in timedOut
        val jobsToDelete: List[Job] = executorIdsToJobs
          .filter{case (id, _) => timedOut.toSet.contains(id)}
          .values
          .toList
        // delete these jobs in one go
        volcanoOperator.jobClient.delete(jobsToDelete: _*)
      }
    }

    if (snapshots.nonEmpty) {
      lastSnapshot = snapshots.last
    }

    // Make a local, non-volatile copy of the reference since it's used multiple times. This
    // is the only method that modifies the list, so this is safe.
    var _deletedExecutorIds = deletedExecutorIds
    if (snapshots.nonEmpty) {
      val existingExecs = lastSnapshot.executorPods.keySet
      _deletedExecutorIds = _deletedExecutorIds.intersect(existingExecs)
    }

    val notDeletedPods = lastSnapshot.executorPods.filterKeys(!_deletedExecutorIds.contains(_))
    // Map the pods into per ResourceProfile id so we can check per ResourceProfile,
    // add a fast path if not using other ResourceProfiles.
    val rpIdToExecsAndPodState =
    mutable.HashMap[Int, mutable.HashMap[Long, ExecutorPodState]]()
    if (totalExpectedExecutorsPerResourceProfileId.size <= 1) {
      rpIdToExecsAndPodState(ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID) =
        mutable.HashMap.empty ++= notDeletedPods
    } else {
      notDeletedPods.foreach { case (execId, execPodState) =>
        val rpId = execPodState.pod.getMetadata.getLabels.get(SPARK_RESOURCE_PROFILE_ID_LABEL).toInt
        val execPods = rpIdToExecsAndPodState.getOrElseUpdate(rpId,
          mutable.HashMap[Long, ExecutorPodState]())
        execPods(execId) = execPodState
      }
    }

    var totalPendingCount = 0
    // The order we request executors for each ResourceProfile is not guaranteed.
    totalExpectedExecutorsPerResourceProfileId.asScala.foreach { case (rpId, targetNum) =>
      val podsForRpId = rpIdToExecsAndPodState.getOrElse(rpId, mutable.HashMap.empty)

      val currentRunningCount = podsForRpId.values.count {
        case PodRunning(_) => true
        case _ => false
      }

      val (schedulerKnownPendingExecsForRpId, currentPendingExecutorsForRpId) = podsForRpId.filter {
        case (_, PodPending(_)) => true
        case _ => false
      }.partition { case (k, _) =>
        schedulerKnownExecs.contains(k)
      }
      // This variable is used later to print some debug logs. It's updated when cleaning up
      // excess pod requests, since currentPendingExecutorsForRpId is immutable.
      var knownPendingCount = currentPendingExecutorsForRpId.size

      val newlyCreatedExecutorsForRpId =
        newlyCreatedExecutors.filter { case (_, (waitingRpId, _)) =>
          rpId == waitingRpId
        }

      val schedulerKnownNewlyCreatedExecsForRpId =
        schedulerKnownNewlyCreatedExecs.filter { case (_, waitingRpId) =>
          rpId == waitingRpId
        }

      if (podsForRpId.nonEmpty) {
        logDebug(s"ResourceProfile Id: $rpId " +
          s"pod allocation status: $currentRunningCount running, " +
          s"${currentPendingExecutorsForRpId.size} unknown pending, " +
          s"${schedulerKnownPendingExecsForRpId.size} scheduler backend known pending, " +
          s"${newlyCreatedExecutorsForRpId.size} unknown newly created, " +
          s"${schedulerKnownNewlyCreatedExecsForRpId.size} scheduler backend known newly created.")
      }
      // It's possible that we have outstanding pods that are outdated when dynamic allocation
      // decides to downscale the application. So check if we can release any pending pods early
      // instead of waiting for them to time out. Drop them first from the unacknowledged list,
      // then from the pending. However, in order to prevent too frequent fluctuation, newly
      // requested pods are protected during executorIdleTimeout period.
      //
      // TODO: with dynamic allocation off, handle edge cases if we end up with more running
      // executors than expected.
      val knownPodCount = currentRunningCount +
        currentPendingExecutorsForRpId.size + schedulerKnownPendingExecsForRpId.size +
        newlyCreatedExecutorsForRpId.size + schedulerKnownNewlyCreatedExecsForRpId.size

      if (knownPodCount > targetNum) {
        val excess = knownPodCount - targetNum
        val newlyCreatedToDelete = newlyCreatedExecutorsForRpId
          .filter { case (_, (_, createTime)) =>
            currentTime - createTime > executorIdleTimeout
          }.keys.take(excess).toList
        val knownPendingToDelete = currentPendingExecutorsForRpId
          .filter(x => isExecutorIdleTimedOut(x._2, currentTime))
          .take(excess - newlyCreatedToDelete.size)
          .map { case (id, _) => id }
        val toDelete = newlyCreatedToDelete ++ knownPendingToDelete

        if (toDelete.nonEmpty) {
          logInfo(s"Deleting ${toDelete.size} excess pod requests (${toDelete.mkString(",")}).")
          _deletedExecutorIds = _deletedExecutorIds ++ toDelete
          // Delete the Volcano Jobs for executor ids in "toDelete" (**not** _deletedExecutorIds)
          // where the job is in pending state
          //
          //           kubernetesClient
          //              .pods()
          //              .withField("status.phase", "Pending")
          //              .withLabel(SPARK_APP_ID_LABEL, applicationId)
          //              .withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE)
          //              .withLabelIn(SPARK_EXECUTOR_ID_LABEL, toDelete.sorted.map(_.toString): _*)
          //              .delete()
          Utils.tryLogNonFatalError {
            deleteJobsForPendingExecutorPods(toDelete.toSet)
            newlyCreatedExecutors --= newlyCreatedToDelete
            knownPendingCount -= knownPendingToDelete.size
          }
        }
      }

      if (newlyCreatedExecutorsForRpId.isEmpty
        && knownPodCount < targetNum) {
        requestNewExecutors(targetNum, knownPodCount, applicationId, rpId)
      }
      totalPendingCount += knownPendingCount

      // The code below just prints debug messages, which are only useful when there's a change
      // in the snapshot state. Since the messages are a little spammy, avoid them when we know
      // there are no useful updates.
      if (log.isDebugEnabled() && snapshots.nonEmpty) {
        val outstanding = knownPendingCount + newlyCreatedExecutorsForRpId.size
        if (currentRunningCount >= targetNum && !dynamicAllocationEnabled) {
          logDebug(s"Current number of running executors for ResourceProfile Id $rpId is " +
            "equal to the number of requested executors. Not scaling up further.")
        } else {
          if (outstanding > 0) {
            logDebug(s"Still waiting for $outstanding executors for ResourceProfile " +
              s"Id $rpId before requesting more.")
          }
        }
      }
    }

    deletedExecutorIds = _deletedExecutorIds

    // Update the flag that helps the setTotalExpectedExecutors() callback avoid triggering this
    // update method when not needed.
    numOutstandingPods.set(totalPendingCount + newlyCreatedExecutors.size)
  }

  private def deleteJobsForPendingExecutorPods(toDelete: Set[Long]): Unit = {
    // Delete the executor jobs for the executor ids in toDelete where the executor pod is
    // in pending state

    // Find the list of all executor pods
    val allJobNames = executorIdsToJobs.values.map { job => job.getMetadata.getName }.toList
    val podsToDelete = kubernetesClient
      .pods()
      .withField("status.phase", "Pending")
      .withLabelIn(VOLCANO_JOB_NAME_LABEL_KEY, allJobNames: _*)
      .list()
      .getItems
      .asScala

    // Get the SPARK_EXECUTOR_ID env var from the first container for each pod
    val pendingExecutorIds = podsToDelete.map {
      pod =>
        val envVars = pod.getSpec.getContainers.head.getEnv
        val executorId = envVars.asScala.filter(env => env.getName.equals(ENV_EXECUTOR_ID)).head.getValue
        executorId.toLong
    }.toSet

    // Filter the jobs by executor ID
    val jobsToDelete = executorIdsToJobs
      .filter { case (id, _) => toDelete.contains(id) && pendingExecutorIds.contains(id) }
      .values
      .toList

    // Delete the jobs in one go
    jobsToDelete.foreach(
      job => logInfo(s"Deleting pending executor for Volcano Job: ${job.getMetadata.getName}")
    )
    volcanoOperator.jobClient.delete(jobsToDelete: _*)
  }

  override def requestNewExecutors(
    expected: Int,
    running: Int,
    applicationId: String,
    resourceProfileId: Int): Unit = {

    for (_ <- running until expected) {
      // allocate a new executor ID
      val newExecutorId = EXECUTOR_ID_COUNTER.incrementAndGet()
      // Create the job for this executor ID
      val execJob: Job = createJob(newExecutorId, resourceProfileId, applicationId)
      executorIdsToJobs(newExecutorId) = execJob

      // Update newlyCreatedExecutors - required for executor state management
      val currentTime = clock.getTimeMillis()
      newlyCreatedExecutors(newExecutorId) = (resourceProfileId, currentTime)
    }
  }
}
