package org.apache.spark.volcano.submit

import io.fabric8.kubernetes.api.model.volcano.batch.Job
import io.fabric8.kubernetes.client.Watcher.Action
import io.fabric8.kubernetes.client.{KubernetesClientException, Watcher}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.KubernetesDriverConf
import org.apache.spark.internal.Logging

import java.net.HttpURLConnection.HTTP_GONE

trait LoggingJobStatusWatcher extends Watcher[Job] {
  def watchOrStop(submissionId: String): Boolean
  def reset(): Unit
}

class LoggingJobStatusWatcherImpl(conf: KubernetesDriverConf)
  extends LoggingJobStatusWatcher with Logging {

  // See https://volcano.sh/en/docs/vcjob/
  private val END_PHASES = Set("aborted", "terminated", "failed", "completed")

  private val appId = conf.appId

  private var jobCompleted = false

  private var resourceTooOldReceived = false

  private var job = Option.empty[Job]

  private def phase: String = {
    val status = job.map(_.getStatus)
    // Some updates contain null status - Some(null)
    if (status.isDefined && status.get != null) {
      val state = status.get.getState
      if (Option(state).isDefined) state.getPhase else "Unknown"
    } else {
      "Unknown"
    }
  }

  override def reset(): Unit = {
    resourceTooOldReceived = false
  }

  override def eventReceived(action: Action, job: Job): Unit = {
    logDebug(s"Received Event: ${job}")
    this.job = Option(job)
    action match {
      case Action.DELETED | Action.ERROR =>
        closeWatch()
      case _ =>
        if (hasCompleted) {
          closeWatch()
        }
    }
  }

  override def onClose(e: KubernetesClientException): Unit = {
    logDebug(s"Stopping watching application $appId with last-observed phase $phase")
    if(e != null && e.getCode == HTTP_GONE) {
      resourceTooOldReceived = true
      logDebug(s"Got HTTP Gone code, resource version changed in k8s api: $e")
    } else {
      closeWatch()
    }
  }

  private def hasCompleted: Boolean = {
    END_PHASES.contains(phase.toLowerCase())
  }

  private def closeWatch(): Unit = synchronized {
    jobCompleted = true
    this.notifyAll()
  }

  override def watchOrStop(sId: String): Boolean = if (conf.get(WAIT_FOR_APP_COMPLETION)) {
    logInfo(s"Waiting for application ${conf.appName} with submission ID $sId to finish...")
    val interval = conf.get(REPORT_INTERVAL)
    synchronized {
      while (!jobCompleted && !resourceTooOldReceived) {
        wait(interval)
        logInfo(s"Application status for $appId (phase: $phase)")
      }
    }

    if(jobCompleted) {
      logInfo(s"Application ${conf.appName} with submission ID $sId finished with phase ${phase}")
    }
    jobCompleted
  } else {
    logInfo(s"Deployed Spark application ${conf.appName} with submission ID $sId into Kubernetes")
    // Always act like the application has completed since we don't want to wait for app completion
    true
  }
}
