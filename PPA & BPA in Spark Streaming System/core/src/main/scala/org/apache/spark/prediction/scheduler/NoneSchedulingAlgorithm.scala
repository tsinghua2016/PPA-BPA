package org.apache.spark.prediction.scheduler

import org.apache.spark.scheduler._

class NoneSchedulingAlgorithm extends SchedulingAlgorithm{
  
 override def comparator(s1: Schedulable, s2: Schedulable): Boolean = {
   return true
  }
  
}
