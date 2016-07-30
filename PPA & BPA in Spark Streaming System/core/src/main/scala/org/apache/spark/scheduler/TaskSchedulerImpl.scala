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

package org.apache.spark.scheduler
import scala.util.control._
import java.nio.ByteBuffer
import java.util
import java.util.{LinkedList, Timer, TimerTask}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.{ArrayBuffer, HashSet}
import scala.language.postfixOps
import scala.util.Random
import org.apache.spark._
import org.apache.spark.TaskState.TaskState
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode
import org.apache.spark.scheduler.TaskLocality.TaskLocality
import org.apache.spark.util.{ThreadUtils, Utils}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.prediction.Prediction
import org.apache.spark.prediction.scheduler._

import scala.collection.mutable
/**
 * Schedules tasks for multiple types of clusters by acting through a SchedulerBackend.
 * It can also work with a local setup by using a LocalBackend and setting isLocal to true.
 * It handles common logic, like determining a scheduling order across jobs, waking up to launch
 * speculative tasks, etc.
 *
 * Clients should first call initialize() and start(), then submit task sets through the
 * runTasks method.
 *
 * THREADING: SchedulerBackends and task-submitting clients can call this class from multiple
 * threads, so it needs locks in public API methods to maintain its state. In addition, some
 * SchedulerBackends synchronize on themselves when they want to send events here, and then
 * acquire a lock on us, so we need to make sure that we don't try to lock the backend while
 * we are holding a lock on ourselves.
 */
private[spark] class TaskSchedulerImpl(
    val sc: SparkContext,
    val maxTaskFailures: Int,
    isLocal: Boolean = false)
  extends TaskScheduler with Logging
{
  def this(sc: SparkContext) = this(sc, sc.conf.getInt("spark.task.maxFailures", 4))

  val conf = sc.conf

  // How often to check for speculative tasks
  val SPECULATION_INTERVAL_MS = conf.getTimeAsMs("spark.speculation.interval", "100ms")

  private val speculationScheduler =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("task-scheduler-speculation")

  // Threshold above which we warn user initial TaskSet may be starved
  val STARVATION_TIMEOUT_MS = conf.getTimeAsMs("spark.starvation.timeout", "15s")

  // CPUs to request per task
  val CPUS_PER_TASK = conf.getInt("spark.task.cpus", 1)

  // TaskSetManagers are not thread safe, so any access to one should be synchronized
  // on this class.
  private val taskSetsByStageIdAndAttempt = new HashMap[Int, HashMap[Int, TaskSetManager]]

  private[scheduler] val taskIdToTaskSetManager = new HashMap[Long, TaskSetManager]
  val taskIdToExecutorId = new HashMap[Long, String]

  @volatile private var hasReceivedTask = false
  @volatile private var hasLaunchedTask = false
  private val starvationTimer = new Timer(true)

  // Incrementing task IDs
  val nextTaskId = new AtomicLong(0)

  // Which executor IDs we have executors on
  val activeExecutorIds = new HashSet[String]

  // The set of executors we have on each host; this is used to compute hostsAlive, which
  // in turn is used to decide when we can attain data locality on a given host
  protected val executorsByHost = new HashMap[String, HashSet[String]]

  protected val hostsByRack = new HashMap[String, HashSet[String]]

  protected val executorIdToHost = new HashMap[String, String]

  // Listener object to pass upcalls into
  var dagScheduler: DAGScheduler = null

  var backend: SchedulerBackend = null

  val mapOutputTracker = SparkEnv.get.mapOutputTracker

  var schedulableBuilder: SchedulableBuilder = null
  var rootPool: Pool = null
  // default scheduler is FIFO
  private val schedulingModeConf = conf.get("spark.scheduler.mode", "FIFO")
  var schedulingMode: SchedulingMode = try {
    SchedulingMode.withName(schedulingModeConf.toUpperCase)
  } catch {
    case e: java.util.NoSuchElementException =>
      throw new SparkException(s"Unrecognized spark.scheduler.mode: $schedulingModeConf")
  }

  // This is a var so that we can reset it for testing purposes.
  private[spark] var taskResultGetter = new TaskResultGetter(sc.env, this)

  override def setDAGScheduler(dagScheduler: DAGScheduler) {
    this.dagScheduler = dagScheduler
  }

  def initialize(backend: SchedulerBackend) {
    this.backend = backend
    // temporarily set rootPool name to empty
  if(Prediction.isCustomize) {
    schedulingMode=Prediction.getSchedulingMode(schedulingMode)
  }
    rootPool = new Pool("", schedulingMode, 0, 0)
    schedulableBuilder = {
      schedulingMode match {
        case SchedulingMode.FIFO =>
          new FIFOSchedulableBuilder(rootPool)
        case SchedulingMode.FAIR =>
          new FairSchedulableBuilder(rootPool, conf)
        case SchedulingMode.CPU =>
          new CPUSchedulableBuilder(rootPool)
        case SchedulingMode.NONE  =>
          new NoneSchedulableBuilder(rootPool)
      }
    }

    schedulableBuilder.buildPools()
  }

  def newTaskId(): Long = nextTaskId.getAndIncrement()

  override def start() {
    backend.start()

    if (!isLocal && conf.getBoolean("spark.speculation", false)) {
      logInfo("Starting speculative execution thread")
      speculationScheduler.scheduleAtFixedRate(new Runnable {
        override def run(): Unit = Utils.tryOrStopSparkContext(sc) {
          checkSpeculatableTasks()
        }
      }, SPECULATION_INTERVAL_MS, SPECULATION_INTERVAL_MS, TimeUnit.MILLISECONDS)
    }
  }

  override def postStartHook() {
    waitBackendReady()
  }

  override def submitTasks(taskSet: TaskSet) {
    val tasks = taskSet.tasks
    logInfo("Adding task set " + taskSet.id + " with " + tasks.length + " tasks")
    this.synchronized {
      val manager = createTaskSetManager(taskSet, maxTaskFailures)
      val stage = taskSet.stageId
      val stageTaskSets =
        taskSetsByStageIdAndAttempt.getOrElseUpdate(stage, new HashMap[Int, TaskSetManager])
      stageTaskSets(taskSet.stageAttemptId) = manager
      val conflictingTaskSet = stageTaskSets.exists { case (_, ts) =>
        ts.taskSet != taskSet && !ts.isZombie
      }
      if (conflictingTaskSet) {
        throw new IllegalStateException(s"more than one active taskSet for stage $stage:" +
          s" ${stageTaskSets.toSeq.map{_._2.taskSet.id}.mkString(",")}")
      }
      schedulableBuilder.addTaskSetManager(manager, manager.taskSet.properties)

      if (!isLocal && !hasReceivedTask) {
        starvationTimer.scheduleAtFixedRate(new TimerTask() {
          override def run() {
            if (!hasLaunchedTask) {
              logWarning("Initial job has not accepted any resources; " +
                "check your cluster UI to ensure that workers are registered " +
                "and have sufficient resources")
            } else {
              this.cancel()
            }
          }
        }, STARVATION_TIMEOUT_MS, STARVATION_TIMEOUT_MS)
      }
      hasReceivedTask = true
    }
   // logWarning(" hasReceivedTask  => "+hasReceivedTask)
    backend.reviveOffers()

  }

  // Label as private[scheduler] to allow tests to swap in different task set managers if necessary
  private[scheduler] def createTaskSetManager(
      taskSet: TaskSet,
      maxTaskFailures: Int): TaskSetManager = {
    new TaskSetManager(this, taskSet, maxTaskFailures)
  }

  override def cancelTasks(stageId: Int, interruptThread: Boolean): Unit = synchronized {
    logInfo("Cancelling stage " + stageId)
    taskSetsByStageIdAndAttempt.get(stageId).foreach { attempts =>
      attempts.foreach { case (_, tsm) =>
        // There are two possible cases here:
        // 1. The task set manager has been created and some tasks have been scheduled.
        //    In this case, send a kill signal to the executors to kill the task and then abort
        //    the stage.
        // 2. The task set manager has been created but no tasks has been scheduled. In this case,
        //    simply abort the stage.
        tsm.runningTasksSet.foreach { tid =>
          val execId = taskIdToExecutorId(tid)
          backend.killTask(tid, execId, interruptThread)
        }
        tsm.abort("Stage %s cancelled".format(stageId))
        logInfo("Stage %d was cancelled".format(stageId))
      }
    }
  }

  /**
   * Called to indicate that all task attempts (including speculated tasks) associated with the
   * given TaskSetManager have completed, so state associated with the TaskSetManager should be
   * cleaned up.
   */
  def taskSetFinished(manager: TaskSetManager): Unit = synchronized {
    taskSetsByStageIdAndAttempt.get(manager.taskSet.stageId).foreach { taskSetsForStage =>
      taskSetsForStage -= manager.taskSet.stageAttemptId
      if (taskSetsForStage.isEmpty) {
        taskSetsByStageIdAndAttempt -= manager.taskSet.stageId
      }
    }
    manager.parent.removeSchedulable(manager)
    logInfo("Removed TaskSet %s, whose tasks have all completed, from pool %s"
      .format(manager.taskSet.id, manager.parent.name))
  }




  /**
    *
    * initialize tasks size
    * load all task
    * */
   private var allTask:LinkedList[TaskDescription]=_
  private var allTasksets:LinkedList[TaskSetManager]=_
  private [spark] def loadAllTask(
         taskSet: TaskSetManager,
         maxLocality: TaskLocality,
         shuffledOffers: Seq[WorkerOffer]):Unit=
  {

    if(allTask==null) allTask=new LinkedList[TaskDescription]()
    if(allTasksets==null) allTasksets=new LinkedList[TaskSetManager];
   // if(Prediction.isload(taskSet.stageId)) return
    for (i <- 0 until shuffledOffers.size) {
      val execId = shuffledOffers(i).executorId
      val host = shuffledOffers(i).host
      try {
            for (task <- taskSet.resourceOffer(execId, host, maxLocality))
            {
              logInfo("task => "+ task)
              if(task!=None && task!=null){
                  allTask.add(task)
                  allTasksets.add(taskSet)
              }
            }

          }
      catch{
        case e: TaskNotSerializableException =>
          logError(s"Load task => Resource offer failed, task set ${taskSet.name} was not serializable")
          return
      }
    }







  }
  /**
    * scheduler
    * */

  /**
    *
    * GPA
    * */
/*private[spark] def GPA(taskSets:ArrayBuffer[TaskSetManager],
                       shuffledOffers: Seq[WorkerOffer],
                         availableCpus: Array[Int],
                     tasks: Seq[ArrayBuffer[TaskDescription]]) =
  {
    for (taskSet <- taskSets; maxLocality <- taskSet.myLocalityLevels) {
      loadAllTask(taskSet, maxLocality, shuffledOffers)
    }
    allTask





  }*/
  /**
    * BPA
    * */



  private[spark] def BPA(
                          taskSet: TaskSetManager,
                          maxLocality: TaskLocality.TaskLocality,
                          shuffledOffers: Seq[WorkerOffer],
                          availableCpus: Array[Int],
                          freemachine:LinkedList[Int],
                          activemachine:LinkedList[Int],
                          extramachine:LinkedList[Int],
                          tasks: Seq[ArrayBuffer[TaskDescription]]
                        ) : Boolean = {


    var launchedTask = false
  /*  var taskconsumes=Prediction.getprediction(taskSet.stageId)
    var sum=0*/
  /*  if(taskconsumes==null) sum=taskSet.tasks.length
    else sum=taskconsumes.sum

    if(sum>100)
    {
      logError(" too large stage with CPU Requirement [ "+sum+" ] stageid  =>"
        +taskSet.stageId+" tasknum =>[" + taskSet.tasks.length+" ]")
      System.exit(0)
      return false
    }
*/
  loadAllTask(taskSet,maxLocality,shuffledOffers)

/*    var select=Prediction.loadstageexecutor(taskSet.stageId)
    if(select == -1) {
      //
        var minleft = Int.MaxValue
        var find = false
        var size = availableCpus.size
        var i = 0
        logInfo("BPA search for fit " + sum + " stage " + taskSet.stageId + " load task number " + stagetasks.length)
        while ((!find) && i < size) {
          var temp = availableCpus(i) - sum
          logInfo("searching [" + i + "] have  " + availableCpus(i))
          if (temp >= 0 && minleft > temp) {
            minleft = temp
            select = i
          }
          if (availableCpus(i) == 100) find = true
          i += 1
        }
      if(select == -1)
      {
        logWarning("BPA not enough core for stage "+taskSet.stageId +"["+sum+"] now")
        return launchedTask
      }
      else{
        logInfo("BPA find "+select+" have "+availableCpus(select))
        Prediction.assignwithstage(taskSet.stageId,select)
        availableCpus(select) -= sum
        logInfo("assign "+sum +" in "+select)
        assert(availableCpus(select) >= 0)
      }
    }*/
    if(allTask.size()==0) {return launchedTask;}
/*    var i=freemachine.iterator()
    while(i.hasNext)
    {
        var j=i.next()
        logInfo("freemachine "+j+" core "+availableCpus(j))
    }

    i=activemachine.iterator()
    while(i.hasNext)
    {
      var j=i.next()
      logInfo("activemachine "+j+" core "+availableCpus(j))
    }

    i=extramachine.iterator()
    while(i.hasNext)
    {
      var j=i.next()
      logInfo("extramachine "+j+" core "+availableCpus(j))
    }*/

    while (allTask.size()>0)
    {
      var task=allTask.getFirst
     val tid = task.taskId
     var select = -1
     var sum=Prediction.getTaskCpuCore(tid)
     logInfo("BPA task consume "+sum)
     if (sum > 50) {
       if (freemachine.size() > 0) {
         select = freemachine.removeFirst()
         activemachine.add(select)
         }
       else if (extramachine.size() > 0) {
         var iter = extramachine.iterator()
         var find = false
         while (!find && iter.hasNext()) {
           var index = iter.next()
           if (availableCpus(index) >= sum) {
             select = index
             find = true
           }
         }
     }// large task assin to extramachinw
   }//large task
   else {

     if (activemachine.size() > 0){
         var index = activemachine.getFirst()
         if (availableCpus(index) >= sum) select = index // use active machine
         else {
           activemachine.removeFirst()
           if (extramachine.size() > 0) {
             index = extramachine.getFirst()
             if (availableCpus(index) >= sum) {
               select = index
             }
             else
               extramachine.removeFirst()

           }//use extramachine

         }
       }// low task find activemachine
     else if(extramachine.size()>0)
       {
         var index = extramachine.getFirst()
         if (availableCpus(index) >= sum) {
           select = index
         }
         else
           extramachine.removeFirst()

       }//use extramachine

     if (select == -1) {
       if (freemachine.size() > 0) {
         select = freemachine.removeFirst()
         extramachine.add(select)
       }
     }//open new machine  record as extramachine
   } // low task assign over
   if (select >= 0) {
     var execId = shuffledOffers(select).executorId
     var host = shuffledOffers(select).host
     try
      {
        logInfo("=> select ["+select+"] core ["+availableCpus(select)+"] fit "+Prediction.getTaskCpuCore(tid))
        availableCpus(select) -= Prediction.getTaskCpuCore(tid)
        assert(availableCpus(select) >= 0)
       tasks(select) += task
        val ts=allTasksets.removeFirst();
       taskIdToTaskSetManager(tid) = ts
       taskIdToExecutorId(tid) = execId
       executorsByHost(host) += execId
       launchedTask = true
        allTask.removeFirst()
   } catch
   {
     case e: TaskNotSerializableException =>
       logError(s"BPA => Resource offer failed, task set ${taskSet.name} was not serializable")
       // Do not offer resources for this task, but don't throw an error to allow other
       // task sets to be submitted.
       return launchedTask
   }
 }//find machine
   else
     {
       //logWarning("BPA not find ");
       if(extramachine.size()>0) 
       {
        var left=availableCpus(extramachine.getFirst())
        Prediction.RePrediction(tid,left)
       }
       else if(activemachine.size()>0)
       {
        var left=availableCpus(activemachine.getFirst())
          Prediction.RePrediction(tid,left)
       }
       return launchedTask
     }
}// end while
    return launchedTask
  }
  /**
    * PPA
    * */
  private[spark] def PPA(
               taskSet: TaskSetManager,
               maxLocality: TaskLocality,
               shuffledOffers: Seq[WorkerOffer],
               availableCpus: Array[Int],
               tasks: Seq[ArrayBuffer[TaskDescription]],
               sort:Boolean
              ) : Boolean = {
    var launchedTask = false

   loadAllTask(taskSet,maxLocality,shuffledOffers)
    logInfo("start stage "+taskSet.stageId)
   // logWarning("load All task over")
    if(allTask.size()==0) {return launchedTask}
    while(allTask.size()>0)
    {

        var task=allTask.getFirst()
        var atindex=0
        if(sort)
        {
          var max=0
          var i=0
          var iter=allTask.iterator()
          while(iter.hasNext)
          {
            var temp=iter.next()
            var c=Prediction.getTaskCpuCore(temp.taskId)
            if(c>max  )
            {
              max=c
              atindex=i
            }
            i += 1
          }
          task=allTask.get(atindex)
          logInfo("sort select task  in "+atindex)
        }
        var tid=task.taskId
        var consume=Prediction.getTaskCpuCore(tid)
        var select = -1
        var minleft=Int.MaxValue
        var index=0
        val loop = new Breaks;
        var maxleft=0
        var maxindex=0
        loop.breakable {
          for (i <- 0 until shuffledOffers.size) {
            index = i
            var temp = availableCpus(i) - consume
            if(availableCpus(i)>maxleft)
            {
              maxleft=availableCpus(i)
              maxindex=i
            }
            if (temp >= 0 && minleft > temp) {
              select = i
              minleft = temp
            }
            //if(availableCpus(i)>=100) loop.break
          }
        }

        if(minleft==Int.MaxValue && select == -1 && index==(shuffledOffers.size-1))
        {
            //logWarning(" => not enough  resource for "+tid +"[ core="+consume+"]")

            if(maxleft==0) return false
            else {
              select=maxindex
              Prediction.RePrediction(tid,maxleft)
            }

        }
        logInfo("=> select ["+select+"] core ["+availableCpus(select)+"] fit "+Prediction.getTaskCpuCore(tid))
        val execId=shuffledOffers(select).executorId
        val host=shuffledOffers(select).host
        tasks(select)+=task
        taskIdToExecutorId(tid) = execId
        executorsByHost(host) += execId
      val ts=allTasksets.removeFirst()
        taskIdToTaskSetManager(tid) = ts
        availableCpus(select) -= Prediction.getTaskCpuCore(tid)
        assert(availableCpus(select) >= 0)
        launchedTask=true
        allTask.removeFirst() //remove task from all task
    }// foreach task
    //logWarning("assign all task")
    return launchedTask
  }


  private def resourceOfferSingleTaskSet(
      taskSet: TaskSetManager,
      maxLocality: TaskLocality,
      shuffledOffers: Seq[WorkerOffer],
      availableCpus: Array[Int],
      tasks: Seq[ArrayBuffer[TaskDescription]]) : Boolean = {
    var launchedTask = false
    logInfo("resourceOfferSingleTaskSet")
    for (i <- 0 until shuffledOffers.size) {
      val execId = shuffledOffers(i).executorId

      val host = shuffledOffers(i).host

      if (availableCpus(i) >= CPUS_PER_TASK) {
          try {
          for (task <- taskSet.resourceOffer(execId, host, maxLocality)) {
            tasks(i) += task
            val tid = task.taskId
            taskIdToTaskSetManager(tid) = taskSet
            taskIdToExecutorId(tid) = execId
            executorsByHost(host) += execId
            availableCpus(i) -= CPUS_PER_TASK
            assert(availableCpus(i) >= 0)
            launchedTask = true
          }
        } catch {
          case e: TaskNotSerializableException =>
            logError(s"Resource offer failed, task set ${taskSet.name} was not serializable")
            // Do not offer resources for this task, but don't throw an error to allow other
            // task sets to be submitted.
            return launchedTask
        }
      }
    }
    return launchedTask
  }

  /**
   * Called by cluster manager to offer resources on slaves. We respond by asking our active task
   * sets for tasks in order of priority. We fill each node with tasks in a round-robin manner so
   * that tasks are balanced across the cluster.
   */
  def resourceOffers(offers: Seq[WorkerOffer]): Seq[Seq[TaskDescription]] = synchronized {
    // Mark each slave as alive and remember its hostname
    // Also track if new executor is added

    var newExecAvail = false
    for (o <- offers) {
      executorIdToHost(o.executorId) = o.host
      activeExecutorIds += o.executorId
      if (!executorsByHost.contains(o.host)) {
        executorsByHost(o.host) = new HashSet[String]()
        executorAdded(o.executorId, o.host)
        newExecAvail = true
      }
      for (rack <- getRackForHost(o.host)) {
        hostsByRack.getOrElseUpdate(rack, new HashSet[String]()) += o.host
      }
    }

    // Randomly shuffle offers to avoid always placing tasks on the same set of workers.
    var shuffledOffers=offers
    //if(!Prediction.isCustomize) 
    shuffledOffers = Random.shuffle(offers)
    // Build a list of tasks to assign to each worker.
    val tasks = shuffledOffers.map(o => new ArrayBuffer[TaskDescription](o.cores))


    val availableCpus = shuffledOffers.map(o => o.cores).toArray
    val sortedTaskSets = rootPool.getSortedTaskSetQueue
    for (taskSet <- sortedTaskSets) {
      logInfo("parentName: %s, name: %s, runningTasks: %s".format(
        taskSet.parent.name, taskSet.name, taskSet.runningTasks))
      if (newExecAvail) {
        taskSet.executorAdded()
      }
    }

    // Take each TaskSet in our scheduling order, and then offer it each node in increasing order
    // of locality levels so that it gets a chance to launch local tasks on all of them.
    // NOTE: the preferredLocality order: PROCESS_LOCAL, NODE_LOCAL, NO_PREF, RACK_LOCAL, ANY
    var launchedTask = false
    logInfo("initialize")
    for(i <-0 until availableCpus.size)
      logInfo("executor ["+i+"] " +availableCpus(i) )
    if(Prediction.isCustomize)
    {
     //  logWarning("Prediction.isCustomize");
        Prediction.gettype match {

          case "PPA" =>
            for (taskSet <- sortedTaskSets; maxLocality <- taskSet.myLocalityLevels) {
              do {
                launchedTask = PPA(
                  taskSet, maxLocality, shuffledOffers, availableCpus, tasks,false)
              } while (launchedTask)

            }
          //  logWarning("PPA break")
          case "BPA" =>
/*            for (taskSet <- sortedTaskSets; maxLocality <- taskSet.myLocalityLevels) {
              do {
                launchedTask = PPA(
                  taskSet, maxLocality, shuffledOffers, availableCpus, tasks,true)
              } while (launchedTask)

            }*/
            val freemachine = new LinkedList[Int]
            val activemachine = new LinkedList[Int]
            val extramachine = new LinkedList[Int]

            for (i <- 0 until availableCpus.size) {
              val core = availableCpus(i)
              if (core >= 100) freemachine.add(i)
              else if (core <= 50 && core > 0) activemachine.add(i)
              else if (core > 50) extramachine.add(i)
            }


            for (taskSet <- sortedTaskSets; maxLocality <- taskSet.myLocalityLevels) {
              do {
                launchedTask = BPA(taskSet, maxLocality, shuffledOffers,
                  availableCpus, freemachine, activemachine, extramachine, tasks
                )
              } while (launchedTask)
            }
           // logWarning("BPA break")
        }//match over

    }//end custome
    else
        for (taskSet <- sortedTaskSets; maxLocality <- taskSet.myLocalityLevels) {
          do {
            launchedTask = resourceOfferSingleTaskSet(
                taskSet, maxLocality, shuffledOffers, availableCpus, tasks)
          } while (launchedTask)
        }

    logInfo("left ")
    for(i <-0 until availableCpus.size)
      logInfo("executor ["+i+"] " +availableCpus(i))

    //logInfo("next scheduler")
    //logWarning("task size => "+tasks.size)
    if (tasks.size > 0) {
      hasLaunchedTask = true
      for(i <- tasks)
        logInfo("run tasks "+i.size)
    }
    //logWarning("haslaunchedtask => "+hasLaunchedTask)
    return tasks
  }

  def statusUpdate(tid: Long, state: TaskState, serializedData: ByteBuffer) {
    var failedExecutor: Option[String] = None
    synchronized {
      try {
        if (state == TaskState.LOST && taskIdToExecutorId.contains(tid)) {
          // We lost this entire executor, so remember that it's gone
          val execId = taskIdToExecutorId(tid)
          if (activeExecutorIds.contains(execId)) {
            removeExecutor(execId)
            failedExecutor = Some(execId)
          }
        }
        taskIdToTaskSetManager.get(tid) match {
          case Some(taskSet) =>
            if (TaskState.isFinished(state)) {
              taskIdToTaskSetManager.remove(tid)
              taskIdToExecutorId.remove(tid)
            }
            if (state == TaskState.FINISHED) {
              taskSet.removeRunningTask(tid)
              taskResultGetter.enqueueSuccessfulTask(taskSet, tid, serializedData)
            } else if (Set(TaskState.FAILED, TaskState.KILLED, TaskState.LOST).contains(state)) {
              taskSet.removeRunningTask(tid)
              taskResultGetter.enqueueFailedTask(taskSet, tid, state, serializedData)
            }
          case None =>
            logError(
              ("Ignoring update with state %s for TID %s because its task set is gone (this is " +
                "likely the result of receiving duplicate task finished status updates)")
                .format(state, tid))
        }
      } catch {
        case e: Exception => logError("Exception in statusUpdate", e)
      }
    }
    // Update the DAGScheduler without holding a lock on this, since that can deadlock
    if (failedExecutor.isDefined) {
      dagScheduler.executorLost(failedExecutor.get)
      backend.reviveOffers()
    }
  }

  /**
   * Update metrics for in-progress tasks and let the master know that the BlockManager is still
   * alive. Return true if the driver knows about the given block manager. Otherwise, return false,
   * indicating that the block manager should re-register.
   */
  override def executorHeartbeatReceived(
      execId: String,
      taskMetrics: Array[(Long, TaskMetrics)], // taskId -> TaskMetrics
      blockManagerId: BlockManagerId): Boolean = {

    val metricsWithStageIds: Array[(Long, Int, Int, TaskMetrics)] = synchronized {
      taskMetrics.flatMap { case (id, metrics) =>
        taskIdToTaskSetManager.get(id).map { taskSetMgr =>
          (id, taskSetMgr.stageId, taskSetMgr.taskSet.stageAttemptId, metrics)
        }
      }
    }
    dagScheduler.executorHeartbeatReceived(execId, metricsWithStageIds, blockManagerId)
  }

  def handleTaskGettingResult(taskSetManager: TaskSetManager, tid: Long): Unit = synchronized {
    taskSetManager.handleTaskGettingResult(tid)
  }

  def handleSuccessfulTask(
      taskSetManager: TaskSetManager,
      tid: Long,
      taskResult: DirectTaskResult[_]): Unit = synchronized {
    //logWarning("handleSuccessfulTask");
    taskSetManager.handleSuccessfulTask(tid, taskResult)
  }

  def handleFailedTask(
      taskSetManager: TaskSetManager,
      tid: Long,
      taskState: TaskState,
      reason: TaskEndReason): Unit = synchronized {
   // logWarning("handleFailedTask");
    taskSetManager.handleFailedTask(tid, taskState, reason)
    if (!taskSetManager.isZombie && taskState != TaskState.KILLED) {
      // Need to revive offers again now that the task set manager state has been updated to
      // reflect failed tasks that need to be re-run.
      backend.reviveOffers()
    }
  }

  def error(message: String) {
    synchronized {
      if (taskSetsByStageIdAndAttempt.nonEmpty) {
        // Have each task set throw a SparkException with the error
        for {
          attempts <- taskSetsByStageIdAndAttempt.values
          manager <- attempts.values
        } {
          try {
            manager.abort(message)
          } catch {
            case e: Exception => logError("Exception in error callback", e)
          }
        }
      } else {
        // No task sets are active but we still got an error. Just exit since this
        // must mean the error is during registration.
        // It might be good to do something smarter here in the future.
        throw new SparkException(s"Exiting due to error from cluster scheduler: $message")
      }
    }
  }

  override def stop() {
    speculationScheduler.shutdown()
    if (backend != null) {
      backend.stop()
    }
    if (taskResultGetter != null) {
      taskResultGetter.stop()
    }
    starvationTimer.cancel()
  }

  override def defaultParallelism(): Int = backend.defaultParallelism()

  // Check for speculatable tasks in all our active jobs.
  def checkSpeculatableTasks() {
    var shouldRevive = false
    synchronized {
      shouldRevive = rootPool.checkSpeculatableTasks()
    }
    if (shouldRevive) {
      backend.reviveOffers()
    }
  }

  override def executorLost(executorId: String, reason: ExecutorLossReason): Unit = {
    var failedExecutor: Option[String] = None

    synchronized {
      if (activeExecutorIds.contains(executorId)) {
        val hostPort = executorIdToHost(executorId)
        logError("Lost executor %s on %s: %s".format(executorId, hostPort, reason))
        removeExecutor(executorId)
        failedExecutor = Some(executorId)
      } else {
         // We may get multiple executorLost() calls with different loss reasons. For example, one
         // may be triggered by a dropped connection from the slave while another may be a report
         // of executor termination from Mesos. We produce log messages for both so we eventually
         // report the termination reason.
         logError("Lost an executor " + executorId + " (already removed): " + reason)
      }
    }
    // Call dagScheduler.executorLost without holding the lock on this to prevent deadlock
    if (failedExecutor.isDefined) {
      dagScheduler.executorLost(failedExecutor.get)
      backend.reviveOffers()
    }
  }

  /** Remove an executor from all our data structures and mark it as lost */
  private def removeExecutor(executorId: String) {
    activeExecutorIds -= executorId
    val host = executorIdToHost(executorId)
    val execs = executorsByHost.getOrElse(host, new HashSet)
    execs -= executorId
    if (execs.isEmpty) {
      executorsByHost -= host
      for (rack <- getRackForHost(host); hosts <- hostsByRack.get(rack)) {
        hosts -= host
        if (hosts.isEmpty) {
          hostsByRack -= rack
        }
      }
    }
    executorIdToHost -= executorId
    rootPool.executorLost(executorId, host)
  }

  def executorAdded(execId: String, host: String) {
    dagScheduler.executorAdded(execId, host)
  }

  def getExecutorsAliveOnHost(host: String): Option[Set[String]] = synchronized {
    executorsByHost.get(host).map(_.toSet)
  }

  def hasExecutorsAliveOnHost(host: String): Boolean = synchronized {
    executorsByHost.contains(host)
  }

  def hasHostAliveOnRack(rack: String): Boolean = synchronized {
    hostsByRack.contains(rack)
  }

  def isExecutorAlive(execId: String): Boolean = synchronized {
    activeExecutorIds.contains(execId)
  }

  // By default, rack is unknown
  def getRackForHost(value: String): Option[String] = None

  private def waitBackendReady(): Unit = {
    if (backend.isReady) {
      return
    }
    while (!backend.isReady) {
      synchronized {
        this.wait(100)
      }
    }
  }

  override def applicationId(): String = backend.applicationId()

  override def applicationAttemptId(): Option[String] = backend.applicationAttemptId()

  private[scheduler] def taskSetManagerForAttempt(
      stageId: Int,
      stageAttemptId: Int): Option[TaskSetManager] = {
    for {
      attempts <- taskSetsByStageIdAndAttempt.get(stageId)
      manager <- attempts.get(stageAttemptId)
    } yield {
      manager
    }
  }

}


private[spark] object TaskSchedulerImpl {
  /**
   * Used to balance containers across hosts.
   *
   * Accepts a map of hosts to resource offers for that host, and returns a prioritized list of
   * resource offers representing the order in which the offers should be used.  The resource
   * offers are ordered such that we'll allocate one container on each host before allocating a
   * second container on any host, and so on, in order to reduce the damage if a host fails.
   *
   * For example, given <h1, [o1, o2, o3]>, <h2, [o4]>, <h1, [o5, o6]>, returns
   * [o1, o5, o4, 02, o6, o3]
   */
  def prioritizeContainers[K, T] (map: HashMap[K, ArrayBuffer[T]]): List[T] = {
    val _keyList = new ArrayBuffer[K](map.size)
    _keyList ++= map.keys

    // order keyList based on population of value in map
    val keyList = _keyList.sortWith(
      (left, right) => map(left).size > map(right).size
    )

    val retval = new ArrayBuffer[T](keyList.size * 2)
    var index = 0
    var found = true

    while (found) {
      found = false
      for (key <- keyList) {
        val containerList: ArrayBuffer[T] = map.get(key).getOrElse(null)
        assert(containerList != null)
        // Get the index'th entry for this host - if present
        if (index < containerList.size){
          retval += containerList.apply(index)
          found = true
        }
      }
      index += 1
    }

    retval.toList
  }

}
