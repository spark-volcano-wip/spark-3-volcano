package org.apache.spark.volcano

import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.api.model.volcano.batch.{Job, JobBuilder, TaskSpec, TaskSpecBuilder}
import org.apache.spark.SparkFunSuite

import scala.collection.JavaConverters._

class JobCreatorSuite extends SparkFunSuite{

  val replicaNumber = 1

  val driverTask = new TaskSpecBuilder()
    .withReplicas(1)
    .withName("driver")
    .build()

  val executorTask = new TaskSpecBuilder()
    .withReplicas(replicaNumber)
    .withName("executors")
    .build()

  val jobBuilder = new JobBuilder().withNewSpec()

  // todo test naming conventions
  def findTask(job: Job, name: String): Option[TaskSpec] = {
    job.getSpec.getTasks.asScala.find(task => task.getName == name)
  }

  test("The executor gets created") {
    val job = JobCreator.executor(
      new Pod(),
      "spark-name",
      3
    )

    assert(findTask(job, "driver").get.getReplicas == 1)
    assert(findTask(job, "executors").get.getReplicas == 3)
    assert(job.getSpec.getTasks.size() == 2)
  }
}
