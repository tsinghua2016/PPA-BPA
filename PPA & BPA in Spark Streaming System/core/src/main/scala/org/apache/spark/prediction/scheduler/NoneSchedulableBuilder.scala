package org.apache.spark.prediction.scheduler

import java.util.Properties

import org.apache.spark.Logging
import org.apache.spark.scheduler.{Pool, Schedulable, SchedulableBuilder}

/**
  * Created by chen on 2016/5/9.
  */

private[spark] class NoneSchedulableBuilder(val rootPool: Pool)
  extends SchedulableBuilder with Logging {

  override def buildPools() {
    // nothing
  }
  override def addTaskSetManager(manager: Schedulable, properties: Properties) {
    logInfo("mode => NoneSchedulableBuilder")
    rootPool.addSchedulable(manager)
  }
}