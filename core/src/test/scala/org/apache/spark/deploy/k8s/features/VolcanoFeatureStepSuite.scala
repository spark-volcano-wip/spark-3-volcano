package org.apache.spark.deploy.k8s.features

import io.fabric8.kubernetes.client.KubernetesClient
import org.apache.spark.SparkFunSuite
import org.mockito.Mock

class VolcanoFeatureStepSuite extends SparkFunSuite {

  var DUMMY_KIND_NAME: String = "jobs"
  var DUMMY_CRD_GROUP = "org.apache.spark.volcano"
  var DUMMY_CRD_NAME: String = DUMMY_KIND_NAME + "." + DUMMY_CRD_GROUP

  @Mock
  private var kubernetesClient: KubernetesClient = _

  test("Test Volcano Custom Resources") {
    

  }

  test("custom resource definitions") {
//    val crdDefinition = new CustomResourceDefinitionBuilder().withNewMetadata.withName("flinkapplications.flink.k8s.io").endMetadata.withNewSpec.withGroup("flink.k8s.io").withVersion("v1alpha1").withNewNames.withKind("FlinkApplication").withPlural("flinkapplications").endNames.withScope("Namespaced").endSpec.build
//
//    val crdContext = new CustomResourceDefinitionContext.Builder().withVersion("v1alpha1").withScope("Namespaced").withGroup("flink.k8s.io").withPlural("flinkapplications").build
  }
}