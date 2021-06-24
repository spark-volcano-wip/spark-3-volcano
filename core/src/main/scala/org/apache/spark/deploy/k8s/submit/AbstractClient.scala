package org.apache.spark.deploy.k8s.submit

import org.apache.spark.internal.Logging

abstract class AbstractClient() extends Logging {
  def run(): Unit
}
