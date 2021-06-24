package org.apache.spark.deploy.k8s.integrationtest

import org.apache.spark.internal.config

private[spark] trait VolcanoSuite { k8sSuite: KubernetesSuite =>

  import VolcanoSuite._
  import KubernetesSuite.k8sTestTag

  test("Test basic decommissioning", k8sTestTag) {
    sparkAppConf
      .set(config.DECOMMISSION_ENABLED.key, "true")
      .set("spark.kubernetes.container.image", pyImage)
      .set(config.STORAGE_DECOMMISSION_ENABLED.key, "true")
      .set(config.STORAGE_DECOMMISSION_SHUFFLE_BLOCKS_ENABLED.key, "true")
      .set(config.STORAGE_DECOMMISSION_RDD_BLOCKS_ENABLED.key, "true")
      .set("spark.executor.instances", "3")
      .set("spark.storage.decommission.replicationReattemptInterval", "1")

    runSparkApplicationAndVerifyCompletion(
      appResource = PYSPARK_DECOMISSIONING,
      mainClass = "",
      expectedDriverLogOnCompletion = Seq(
        "Finished waiting, stopping Spark",
        "Decommission executors",
        "Final accumulator value is: 100"),
      appArgs = Array.empty[String],
      driverPodChecker = doBasicDriverPyPodCheck,
      executorPodChecker = doBasicExecutorPyPodCheck,
      appLocator = appLocator,
      isJVM = false,
      pyFiles = None,
      executorPatience = None,
      decommissioningTest = true)
  }

//  test("Test basic decommissioning with shuffle cleanup", k8sTestTag) {
//    sparkAppConf
//      .set(config.DECOMMISSION_ENABLED.key, "true")
//      .set("spark.kubernetes.container.image", pyImage)
//      .set(config.STORAGE_DECOMMISSION_ENABLED.key, "true")
//      .set(config.STORAGE_DECOMMISSION_SHUFFLE_BLOCKS_ENABLED.key, "true")
//      .set(config.STORAGE_DECOMMISSION_RDD_BLOCKS_ENABLED.key, "true")
//      .set(config.DYN_ALLOCATION_SHUFFLE_TRACKING_ENABLED.key, "true")
//      .set(config.DYN_ALLOCATION_SHUFFLE_TRACKING_TIMEOUT.key, "400")
//      // Ensure we have somewhere to migrate our data too
//      .set("spark.executor.instances", "3")
//      // The default of 30 seconds is fine, but for testing we just want to get this done fast.
//      .set("spark.storage.decommission.replicationReattemptInterval", "1")
//
//    runSparkApplicationAndVerifyCompletion(
//      appResource = PYSPARK_DECOMISSIONING_CLEANUP,
//      mainClass = "",
//      expectedDriverLogOnCompletion = Seq(
//        "Finished waiting, stopping Spark",
//        "Decommission executors"),
//      appArgs = Array.empty[String],
//      driverPodChecker = doBasicDriverPyPodCheck,
//      executorPodChecker = doBasicExecutorPyPodCheck,
//      appLocator = appLocator,
//      isJVM = false,
//      pyFiles = None,
//      executorPatience = None,
//      decommissioningTest = true)
//  }
//
//  test("Test decommissioning with dynamic allocation & shuffle cleanups", k8sTestTag) {
//    sparkAppConf
//      .set(config.DECOMMISSION_ENABLED.key, "true")
//      .set("spark.kubernetes.container.image", pyImage)
//      .set(config.STORAGE_DECOMMISSION_ENABLED.key, "true")
//      .set(config.STORAGE_DECOMMISSION_SHUFFLE_BLOCKS_ENABLED.key, "true")
//      .set(config.STORAGE_DECOMMISSION_RDD_BLOCKS_ENABLED.key, "true")
//      .set(config.DYN_ALLOCATION_SHUFFLE_TRACKING_ENABLED.key, "true")
//      .set(config.DYN_ALLOCATION_SHUFFLE_TRACKING_TIMEOUT.key, "30")
//      .set(config.DYN_ALLOCATION_CACHED_EXECUTOR_IDLE_TIMEOUT.key, "30")
//      .set(config.DYN_ALLOCATION_EXECUTOR_IDLE_TIMEOUT.key, "5")
//      .set(config.DYN_ALLOCATION_MIN_EXECUTORS.key, "1")
//      .set(config.DYN_ALLOCATION_INITIAL_EXECUTORS.key, "2")
//      .set(config.DYN_ALLOCATION_ENABLED.key, "true")
//      // The default of 30 seconds is fine, but for testing we just want to get this done fast.
//      .set("spark.storage.decommission.replicationReattemptInterval", "1")
//
//    var execLogs: String = ""
//
//    runSparkApplicationAndVerifyCompletion(
//      appResource = PYSPARK_SCALE,
//      mainClass = "",
//      expectedDriverLogOnCompletion = Seq(
//        "Finished waiting, stopping Spark",
//        "Decommission executors"),
//      appArgs = Array.empty[String],
//      driverPodChecker = doBasicDriverPyPodCheck,
//      executorPodChecker = doBasicExecutorPyPodCheck,
//      appLocator = appLocator,
//      isJVM = false,
//      pyFiles = None,
//      executorPatience = None,
//      decommissioningTest = false)
//  }
}

private[spark] object VolcanoSuite {
  val TEST_LOCAL_PYSPARK: String = "local:///opt/spark/tests/"
  val PYSPARK_DECOMISSIONING: String = TEST_LOCAL_PYSPARK + "decommissioning.py"
  val PYSPARK_DECOMISSIONING_CLEANUP: String = TEST_LOCAL_PYSPARK + "decommissioning_cleanup.py"
  val PYSPARK_SCALE: String = TEST_LOCAL_PYSPARK + "autoscale.py"
}
