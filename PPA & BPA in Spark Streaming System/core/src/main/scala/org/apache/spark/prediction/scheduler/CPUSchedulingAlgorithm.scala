package org.apache.spark.prediction.scheduler

import org.apache.spark.prediction.Prediction
import org.apache.spark.scheduler._

class CPUSchedulingAlgorithm extends SchedulingAlgorithm{
  
 override def comparator(s1: Schedulable, s2: Schedulable): Boolean = {
   val a1=Prediction.getprediction(s1.stageId)
   val a2=Prediction.getprediction(s2.stageId)
   var a1sum=0
   var a2sum=0
   if(a1!=null) a1sum=a1.sum
   else a1sum = s1.asInstanceOf[TaskSetManager].tasks.length
   if(a2!=null) a2sum=a2.sum
   else a2sum = s2.asInstanceOf[TaskSetManager].tasks.length
   var res=a1sum-a2sum
    if(res>0) return true
    else return false
    
  }
  
}
