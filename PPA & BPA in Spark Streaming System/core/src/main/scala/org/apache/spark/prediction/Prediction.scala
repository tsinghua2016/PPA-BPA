package org.apache.spark.prediction

import java.util.HashMap

import org.apache.spark.{Logging, SparkConf, SparkException}
import org.apache.spark.scheduler.{SchedulingMode, Stage}
import java.io._

import scala.collection.mutable.HashSet
import org.apache.spark.scheduler.SchedulingMode._
import org.apache.spark.scheduler.Stage

import scala.collection.mutable


object Prediction extends Logging
{
  private var _map:HashMap[String,TaskPrediction]= _
  private var _conf:SparkConf = _
  private var _open:Boolean=false
  private var stagetotask:HashMap[Integer,Array[Int]]=_
  private var taskIdtocpu:HashMap[Long,Integer]=_
  private var taskdefaultcpu:Int= _
  private var stagetoexecutor:HashMap[Integer,Integer]=_
  private var loadstage:HashSet[Integer]=_
  private var stagetaskload:HashMap[Integer,Array[Boolean]]=_

  private[spark] def prediction(stage:Stage)=
  {
    var key=stage.name
    var num=stage.numTasks
  }
  private[spark] def isCustomize=_open
  private[spark] def getmap=_map

  private[spark] def init(conf:SparkConf,appName:String):Unit=
  {
    _conf=conf
    _open=_conf.getBoolean("spark.customize.setcustomize",false)
    if(! _open) {
      logInfo("spark.customize.setcustomize => false");
      return
    }
    stagetotask=new HashMap[Integer,Array[Int]]();
    stagetaskload=new HashMap[Integer,Array[Boolean]]();
    taskIdtocpu=new HashMap[Long,Integer]();
    taskdefaultcpu=_conf.getInt("spark.task.cpus", 1)
    if(taskdefaultcpu>100) taskdefaultcpu=100
    logInfo("spark.customize.setcustomize  => true");
    val path=_conf.get("spark.customize.scheduler.filedirpath","./")
    logInfo("File path => "+path);
    try {

      val savefile=new File(path, appName+".obj")
      val fi = new FileInputStream(savefile)
      val ow = new ObjectInputStream(fi)
      _map =  ow.readObject().asInstanceOf[HashMap[String,TaskPrediction]]
      logInfo(appName+" Prediction initialize Success !");
      ow.close()
      fi.close()
    }
    catch {
      case ex:Exception => {
        ex.printStackTrace()
        logError(appName+" Prediction initialize failed");
        System.exit(0);
      }

    }

  }
  private [spark] def isload(stageid:Integer,index:Integer):Boolean=
  {
    var flag=false
    if(stagetaskload.containsKey(stageid))
      {
        var sc=stagetaskload.get(stageid)
        flag=sc(index)
      }
    flag
  }
  private [spark]  def marktaskload(stageid:Integer,index:Integer,num:Integer)=
    if(isCustomize)
      {
    synchronized {
      var sc: Array[Boolean] = null
      if (stagetaskload.containsValue(stageid)) {
        sc = stagetaskload.get(stageid)
        sc(index) = true
      }
      else {
        sc = new Array[Boolean](num)
        for (i <- 0 until num)
          sc(i) = false
        sc(index) = true
      }
      stagetaskload.put(stageid, sc)
    }
  }
  private [spark] def RePrediction(taskid:Long,core:Int)
  =synchronized{taskIdtocpu.put(taskid,core)}
 private [spark] def markstage(stageid:Integer)={
    if(loadstage==null) loadstage=new mutable.HashSet[Integer]()
    loadstage.add(stageid)
   logInfo("load stage "+stageid+" over")
  }
private [spark] def isload(stageid:Integer):Boolean={
  if(loadstage==null) return false
  loadstage.contains(stageid)
}

  private[spark] def stagePrediction(stage:Stage):Unit=
  {
    if(!_open) return

    var key=stage.name
    var tp=_map.get(key)
    if(tp==null) {
      logInfo("use default plan to predicte stage ["+stage.name+"]")
      return
    }
    var tasknum=stage.numTasks
    var tc =new Array[Int](tasknum)
    logInfo(
      "name => "+stage.name+
        " id => "+stage.id+" "+
        " numPartitions => "+stage.numPartitions+
        " numtasks => "+stage.numTasks)

    for(i <- 0 until tasknum)
      tc(i)=tp.prediction();

    var msg="";
    tc.foreach(cpu=>msg=msg+" "+cpu)
    logInfo("predict stage ["+stage.id+"] with "+tasknum+" tasks in ["+msg+"]")
    synchronized{
      stagetotask.put(stage.id, tc)
    }

  }
  private[spark] def removeStage(stageid:Int)=
    if(_open &&stagetotask.containsKey(stageid)){
      synchronized{
        stagetotask.remove(stageid)
        //loadstage.remove(stageid)
      }
      logInfo("remove stage ["+stageid+"]")
    }


  private[spark] def getprediction(stageid:Int):Array[Int]=
    stagetotask.get(stageid)
  private[spark] def addtaskId(taskid:Long,stageid:Int,index:Int)=
    if(_open)
    {
     if(stagetotask.containsKey(stageid))
       taskdefaultcpu=stagetotask.get(stageid)(index)
      synchronized{taskIdtocpu.put(taskid,taskdefaultcpu)}
      logInfo("add task ["+taskid+"] with cpu="+taskdefaultcpu+" to taskIdtocpu")
    }

  private[spark] def removeTaskId(taskid:Long)=
    if(_open){
      synchronized{taskIdtocpu.remove(taskid)}
      logInfo("remove task ["+taskid+"] from taskIdtocpu")
    }
  private[spark]  def getTaskCpuCore(taskid:Long):Int=
   synchronized{ taskIdtocpu.getOrDefault(taskid,taskdefaultcpu)}

  private[spark] def gettype= _conf.get("spark.customize.scheduler.mode","PPA").toUpperCase()

  //taskset scheduler
  private[spark] def getSchedulingMode(schedulingMode: SchedulingMode):SchedulingMode=
  {
    if(_open)
    {
      val schedulingModeConf:String=_conf.get("spark.customize.scheduler.mode","PPA")
      val customMode={
        schedulingModeConf.toUpperCase() match {
          case "PPA" => SchedulingMode.NONE
          case "BPA" => SchedulingMode.NONE
          case "GPA" => SchedulingMode.NONE
          case _ =>
            throw new SparkException(s"Unrecognized spark.customize.scheduler.mode: $schedulingModeConf")
        }
      }
      customMode
    }
    else
      schedulingMode
  }




}